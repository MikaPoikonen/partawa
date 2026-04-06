import os
import requests
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer

SHOP = os.getenv("SHOPIFY_STORE_DOMAIN")
CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

def get_access_token():
    url = f"https://{SHOP}/admin/oauth/access_token"
    response = requests.post(url, json={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    })
    return response.json()["access_token"]

def get_products(token):
    url = f"https://{SHOP}/admin/api/{API_VERSION}/products.json"
    headers = {
        "X-Shopify-Access-Token": token
    }
    response = requests.get(url, headers=headers)
    return response.json()["products"]

def generate_xml(products):
    root = ET.Element("products")

    for product in products:
        for variant in product["variants"]:
            p = ET.SubElement(root, "product")

            ET.SubElement(p, "title").text = product["title"]
            ET.SubElement(p, "description").text = product["body_html"]
            ET.SubElement(p, "link").text = f"https://partawa.fi/products/{product['handle']}"
            ET.SubElement(p, "price").text = variant["price"]
            ET.SubElement(p, "sku").text = variant.get("sku", "")
            ET.SubElement(p, "brand").text = product.get("vendor", "Partawa")

            if product.get("image"):
                ET.SubElement(p, "default_image").text = product["image"]["src"]

            ET.SubElement(p, "in_stock").text = "true" if variant["available"] else "false"

    return ET.tostring(root, encoding="utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/daisycon-feed.xml":
            token = get_access_token()
            products = get_products(token)
            xml = generate_xml(products)

            self.send_response(200)
            self.send_header("Content-type", "application/xml")
            self.end_headers()
            self.wfile.write(xml)
        else:
            self.send_response(404)
            self.end_headers()

def run():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()

if __name__ == "__main__":
    run()
