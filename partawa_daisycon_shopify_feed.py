#!/usr/bin/env python3
import os
import json
import urllib.request
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer

SHOP = os.getenv("SHOPIFY_STORE_DOMAIN")
TOKEN = os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

QUERY = """
{
  products(first: 50) {
    nodes {
      title
      handle
      descriptionHtml
      vendor
      productType
      featuredImage {
        url
      }
      images(first: 5) {
        nodes {
          url
        }
      }
      variants(first: 50) {
        nodes {
          title
          sku
          barcode
          price
          compareAtPrice
          inventoryQuantity
        }
      }
    }
  }
}
"""

def fetch_products():
    req = urllib.request.Request(
        GRAPHQL_URL,
        data=json.dumps({"query": QUERY}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": TOKEN,
        },
    )

    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read().decode())
        return data["data"]["products"]["nodes"]

def build_xml(products):
    root = ET.Element("products")

    for product in products:
        for variant in product["variants"]["nodes"]:
            p = ET.SubElement(root, "product")

            title = product["title"]
            if variant["title"] != "Default Title":
                title += f" - {variant['title']}"

            ET.SubElement(p, "title").text = title
            ET.SubElement(p, "description").text = product["descriptionHtml"]
            ET.SubElement(p, "link").text = f"https://partawa.fi/products/{product['handle']}"
            ET.SubElement(p, "price").text = variant["price"]

            if variant.get("compareAtPrice"):
                ET.SubElement(p, "price_old").text = variant["compareAtPrice"]

            ET.SubElement(p, "category").text = product.get("productType", "")
            ET.SubElement(p, "category_path").text = product.get("vendor", "")

            ET.SubElement(p, "sku").text = variant.get("sku", "")

            if variant.get("barcode"):
                ET.SubElement(p, "ean").text = variant["barcode"]

            ET.SubElement(p, "brand").text = product.get("vendor", "")

            if product.get("featuredImage"):
                ET.SubElement(p, "default_image").text = product["featuredImage"]["url"]

            for img in product["images"]["nodes"]:
                ET.SubElement(p, "detail_images").text = img["url"]

            ET.SubElement(p, "condition").text = "new"
            ET.SubElement(p, "delivery_time").text = "2"

            in_stock = "true" if variant.get("inventoryQuantity", 0) > 0 else "false"
            ET.SubElement(p, "in_stock").text = in_stock

    return ET.tostring(root, encoding="utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/daisycon-feed.xml":
            try:
                products = fetch_products()
                xml = build_xml(products)

                self.send_response(200)
                self.send_header("Content-type", "application/xml")
                self.end_headers()
                self.wfile.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
                self.wfile.write(xml)

            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())

        else:
            self.send_response(404)
            self.end_headers()

def run():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Running on port {port}")
    server.serve_forever()

if __name__ == "__main__":
    run()
