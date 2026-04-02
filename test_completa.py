"""Testa se baixar-completa retorna PDF direto sem email."""
import httpx

# Login
r = httpx.post("https://api-agencia.amazonasenergia.com/api/autenticacao/login",
    json={"CPF_CNPJ": "97185892287", "SENHA": "Iria571991"},
    headers={"Content-Type": "application/json", "User-Agent": "okhttp/4.9.0"})
data = r.json()
token = data["TOKEN"]
print(f"Login OK!")

headers = {
    "Authorization": f"Bearer {token}",
    "X-Client-Id": "15660591",
    "X-Consumer-Unit": "21300",
    "Content-Type": "application/json",
    "User-Agent": "okhttp/4.9.0",
}

# Teste 1: sem email
print("\n--- Teste 1: SEM email ---")
r1 = httpx.post("https://api-agencia.amazonasenergia.com/api/faturas/baixar-completa",
    headers=headers,
    json={"MES_ANO": "2026-01-01", "FATURA_DIVERSA": 0})
print(f"Status: {r1.status_code}")
print(f"Content-Type: {r1.headers.get('content-type', '?')}")
print(f"Size: {len(r1.content)} bytes")
print(f"Body: {r1.text[:300]}")

# Teste 2: com email
print("\n--- Teste 2: COM email ---")
r2 = httpx.post("https://api-agencia.amazonasenergia.com/api/faturas/baixar-completa",
    headers=headers,
    json={"MES_ANO": "2026-01-01", "FATURA_DIVERSA": 0, "EMAIL": "sergiocordeirorabelo@gmail.com"})
print(f"Status: {r2.status_code}")
print(f"Content-Type: {r2.headers.get('content-type', '?')}")
print(f"Size: {len(r2.content)} bytes")
print(f"Body: {r2.text[:300]}")

# Teste 3: via mobile (sem Origin/Referer)
print("\n--- Teste 3: Mobile headers ---")
r3 = httpx.post("https://api-agencia.amazonasenergia.com/api/faturas/baixar-completa",
    headers={
        "Authorization": f"Bearer {token}",
        "X-Client-Id": "15660591",
        "X-Consumer-Unit": "21300",
        "Content-Type": "application/json",
        "User-Agent": "okhttp/4.9.0",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive",
    },
    json={"MES_ANO": "2026-01-01", "FATURA_DIVERSA": 0, "EMAIL": "sergiocordeirorabelo@gmail.com"})
print(f"Status: {r3.status_code}")
print(f"Size: {len(r3.content)} bytes")
print(f"Body: {r3.text[:300]}")
