#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import html
import json
import os
import re
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Tuple

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "partawa.fi").strip()
SHOPIFY_ADMIN_ACCESS_TOKEN = os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01").strip()
DEFAULT_BRAND = os.getenv("DEFAULT_BRAND", "Partawa").strip() or "Partawa"
DEFAULT_DELIVERY_TIME = os.getenv("DEFAULT_DELIVERY_TIME", "2").strip()
DEFAULT_SHIPPING_PRICE = os.getenv("DEFAULT_SHIPPING_PRICE", "0").strip()
DEFAULT_CONDITION = os.getenv("DEFAULT_CONDITION", "new").strip() or "new"
PORT = int(os.getenv("PORT", "10000"))

GRAPHQL_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

PRODUCTS_QUERY = """
query ProductsPage($cursor: String) {
  products(first: 50, after: $cursor, sortKey: UPDATED_AT, reverse: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      handle
      title
      descriptionHtml
      vendor
      productType
      status
      publishedAt
      featuredImage {
        url
      }
      images(first: 10) {
        nodes {
          url
        }
      }
      variants(first: 100) {
        nodes {
          id
          title
          sku
          barcode
          price
          compareAtPrice
          inventoryQuantity
          inventoryPolicy
          image {
            url
          }
          selectedOptions {
            name
            value
          }
        }
      }
    }
  }
}
"""

@dataclass
class FeedItem:
    title: str
    description: str
    link: str
    price: str
    sku: str
    default_image: str
    detail_images: List[str]
    brand: str
    category: str
    category_path: str
    condition: str
    delivery_time: Optional[str]
    ean: str
    in_stock: str
    in_stock_amount: Optional[str]
    price_old: Optional[str]
    price_shipping: Optional[str]
    google_category_id: Optional[str]
    size: Optional[str]

def strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def clean_url(url: str) -> str:
    if not url:
        return ""
    return url.split("?", 1)[0].strip()

def normalize_decimal(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    s = str(value).strip().replace(",", ".")
    try:
        num = float(s)
    except ValueError:
        return s
    return f"{num:.2f}"

def valid_ean(value: str) -> bool:
    value = (value or "").strip()
    return bool(re.fullmatch(r"\d{8}|\d{12}|\d{13}|\d{14}", value))

def option_value(selected_options: Iterable[Dict[str, Any]], names: Iterable[str]) -> Optional[str]:
    lookup = {
        str(opt.get("name", "")).strip().lower(): str(opt.get("value", "")).strip()
        for opt in selected_options or []
    }
    for name in names:
        value = lookup.get(name.lower())
        if value and value.lower() != "default title":
            return value
    return None

def build_product_url(handle: str) -> str:
    return f"https://{SHOPIFY_STORE_DOMAIN}/products/{handle}"

def category_from_product(product: Dict[str, Any]) -> str:
    product_type = (product.get("productType") or "").strip()
    vendor = (product.get("vendor") or DEFAULT_BRAND).strip()
    return product_type or vendor or DEFAULT_BRAND

def category_path_from_product(product: Dict[str, Any]) -> str:
    vendor = (product.get("vendor") or DEFAULT_BRAND).strip()
    category = category_from_product(product)
    if vendor and category and vendor != category:
        return f"{vendor}|{category}"
    return category or vendor or DEFAULT_BRAND

def pick_images(product: Dict[str, Any], variant: Dict[str, Any]) -> Tuple[str, List[str]]:
    images: List[str] = []
    variant_img = ((variant.get("image") or {}).get("url") or "").strip()
    featured_img = ((product.get("featuredImage") or {}).get("url") or "").strip()

    if variant_img:
        images.append(clean_url(variant_img))
    if featured_img and clean_url(featured_img) not in images:
        images.append(clean_url(featured_img))

    for img in (product.get("images") or {}).get("nodes", []) or []:
        url = clean_url((img or {}).get("url") or "")
        if url and url not in images:
            images.append(url)

    default_image = images[0] if images else ""
    detail_images = images[1:]
    return default_image, detail_images

def variant_in_stock(variant: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    qty = variant.get("inventoryQuantity")
    policy = str(variant.get("inventoryPolicy") or "").upper()
    if isinstance(qty, int):
        if qty > 0:
            return "true", str(qty)
        if policy == "CONTINUE":
            return "true", str(qty)
        return "false", str(qty)
    return ("true", None) if policy == "CONTINUE" else ("false", None)

def graphql_request(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = urllib.request.Request(
        GRAPHQL_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Shopify-Access-Token": SHOPIFY_ADMIN_ACCESS_TOKEN,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Shopify API HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Shopify API connection failed: {e}") from e

    data = json.loads(body)
    if data.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")
    return data["data"]

def fetch_all_products() -> List[Dict[str, Any]]:
    if not SHOPIFY_ADMIN_ACCESS_TOKEN:
        raise RuntimeError("Missing SHOPIFY_ADMIN_ACCESS_TOKEN environment variable.")

    all_products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        data = graphql_request(PRODUCTS_QUERY, {"cursor": cursor})
        products = data["products"]["nodes"]
        page_info = data["products"]["pageInfo"]
        all_products.extend(products)

        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

    return all_products

def build_feed_items(products: List[Dict[str, Any]]) -> List[FeedItem]:
    items: List[FeedItem] = []

    for product in products:
        status = str(product.get("status") or "").upper()
        if status != "ACTIVE":
            continue
        if not product.get("publishedAt"):
            continue

        base_title = (product.get("title") or "").strip()
        description = strip_html(product.get("descriptionHtml") or "")
        brand = (product.get("vendor") or DEFAULT_BRAND).strip() or DEFAULT_BRAND
        product_link = clean_url(build_product_url(product.get("handle") or ""))
        category = category_from_product(product)
        category_path = category_path_from_product(product)

        for variant in (product.get("variants") or {}).get("nodes", []) or []:
            selected_options = variant.get("selectedOptions") or []
            size = option_value(selected_options, ["size", "koko"])
            variant_title = (variant.get("title") or "").strip()

            title = base_title
            if variant_title and variant_title.lower() != "default title":
                title = f"{base_title} - {variant_title}"

            sku = (variant.get("sku") or "").strip()
            price = normalize_decimal(variant.get("price"))
            price_old = normalize_decimal(variant.get("compareAtPrice")) or None
            ean_raw = (variant.get("barcode") or "").strip()
            ean = ean_raw if valid_ean(ean_raw) else ""
            default_image, detail_images = pick_images(product, variant)
            in_stock, in_stock_amount = variant_in_stock(variant)

            item = FeedItem(
                title=title,
                description=description,
                link=product_link,
                price=price,
                sku=sku or str(variant.get("id") or "").split("/")[-1],
                default_image=default_image,
                detail_images=detail_images,
                brand=brand,
                category=category,
                category_path=category_path,
                condition=DEFAULT_CONDITION,
                delivery_time=DEFAULT_DELIVERY_TIME or None,
                ean=ean,
                in_stock=in_stock,
                in_stock_amount=in_stock_amount,
                price_old=price_old,
                price_shipping=DEFAULT_SHIPPING_PRICE or None,
                google_category_id=None,
                size=size,
            )

            if item.title and item.description and item.link and item.price and item.default_image:
                items.append(item)

    return items

def xml_add(parent: ET.Element, tag: str, value: Optional[str]) -> None:
    if value is None or value == "":
        return
    child = ET.SubElement(parent, tag)
    child.text = str(value)

def build_xml(feed_items: List[FeedItem]) -> bytes:
    root = ET.Element("products")

    for item in feed_items:
        p = ET.SubElement(root, "product")
        xml_add(p, "title", item.title)
        xml_add(p, "description", item.description)
        xml_add(p, "link", item.link)
        xml_add(p, "price", item.price)
        xml_add(p, "price_old", item.price_old)
        xml_add(p, "category", item.category)
        xml_add(p, "category_path", item.category_path)
        xml_add(p, "sku", item.sku)
        xml_add(p, "ean", item.ean)
        xml_add(p, "size", item.size)
        xml_add(p, "brand", item.brand)
        xml_add(p, "default_image", item.default_image)
        for image_url in item.detail_images:
            xml_add(p, "detail_images", image_url)
        xml_add(p, "condition", item.condition)
        xml_add(p, "delivery_time", item.delivery_time)
        xml_add(p, "in_stock", item.in_stock)
        xml_add(p, "in_stock_amount", item.in_stock_amount)
        xml_add(p, "price_shipping", item.price_shipping)
        xml_add(p, "google_category_id", item.google_category_id)

    xml_bytes = ET.tostring(root, encoding="utf-8")
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + xml_bytes

_CACHE_XML: Optional[bytes] = None
_CACHE_GENERATED_AT: Optional[dt.datetime] = None
_CACHE_SECONDS = 300

def get_feed_xml() -> Tuple[bytes, dt.datetime]:
    global _CACHE_XML, _CACHE_GENERATED_AT
    now = dt.datetime.now(dt.timezone.utc)

    if (
        _CACHE_XML is not None
        and _CACHE_GENERATED_AT is not None
        and (now - _CACHE_GENERATED_AT).total_seconds() < _CACHE_SECONDS
    ):
        return _CACHE_XML, _CACHE_GENERATED_AT

    products = fetch_all_products()
    feed_items = build_feed_items(products)
    xml_bytes = build_xml(feed_items)

    _CACHE_XML = xml_bytes
    _CACHE_GENERATED_AT = now
    return xml_bytes, now

class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in ("/", "/daisycon-feed.xml"):
            try:
                xml_bytes, generated_at = get_feed_xml()
            except Exception as exc:
                body = f"Feed generation failed: {exc}\n".encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(xml_bytes)))
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("Last-Modified", format_datetime(generated_at, usegmt=True))
            self.end_headers()
            self.wfile.write(xml_bytes)
            return

        if self.path == "/healthz":
            body = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = b"Not found\n"
        self.send_response(404)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Serving on http://0.0.0.0:{PORT}/daisycon-feed.xml")
    server.serve_forever()

if __name__ == "__main__":
    main()
