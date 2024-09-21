# rotating proxy via NodeMaven
USER = "lucashobart4_gmail_com-country-us"
PASS = "zlhdjg2kag"
HOST = "gate.nodemaven.com:8080"
PROXY_OPTIONS = { "http": f"http://{USER}:{PASS}@{HOST}" }
PROXY = f"http://{USER}:{PASS}@{HOST}"