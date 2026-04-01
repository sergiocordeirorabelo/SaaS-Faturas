"""Teste do Scraping Browser BrightData com bypass de password."""
import asyncio
from playwright.async_api import async_playwright

SBR_WSS = "wss://brd-customer-hl_1e5dc1ab-zone-login_faturas:zlabsu83uo74@brd.superproxy.io:9222"

async def test():
    async with async_playwright() as p:
        print("Conectando ao Scraping Browser...")
        browser = await p.chromium.connect_over_cdp(SBR_WSS)
        page = browser.contexts[0].pages[0] if browser.contexts[0].pages else await browser.contexts[0].new_page()
        page.set_default_timeout(60000)

        print("Acessando portal...")
        await page.goto("https://agencia.amazonasenergia.com", wait_until="domcontentloaded", timeout=120000)
        await asyncio.sleep(8)

        print("Digitando CPF...")
        await page.click("input#CPF_CNPJ", timeout=30000)
        await page.keyboard.type("97185892287", delay=80)
        await asyncio.sleep(1)

        print("Truque: password -> text...")
        await page.evaluate('document.querySelector("input#SENHA").setAttribute("type", "text")')
        await asyncio.sleep(1)

        print("Digitando senha...")
        await page.click("input#SENHA", timeout=30000)
        await page.keyboard.type("Iria571991", delay=80)
        await asyncio.sleep(2)

        print("Clicando checkbox...")
        clicked = False
        for sel in ["text=Não sou um robô", "text=Nao sou um robo", "input[type='checkbox']", "label >> text=rob"]:
            try:
                await page.click(sel, timeout=5000)
                clicked = True
                print(f"Checkbox clicado via: {sel}")
                break
            except Exception:
                continue

        if not clicked:
            print("Checkbox nao encontrado. BrightData CAPTCHA Solver pode resolver sozinho...")

        # Aguarda o CAPTCHA Solver do BrightData + token aparecer
        print("Aguardando captcha resolver...")
        for i in range(20):
            token = await page.evaluate('(document.querySelector("textarea[name=g-recaptcha-response]") || {}).value || ""')
            if token:
                print(f"Token encontrado! Length: {len(token)}")
                break
            await asyncio.sleep(3)
            print(f"  Aguardando... {(i+1)*3}s")

        print("Clicando Entrar...")
        try:
            async with page.expect_response(
                lambda r: "autenticacao/login" in r.url, timeout=60000
            ) as resp_info:
                await page.click("button[type=submit]", timeout=30000)
            response = await resp_info.value
            print(f"STATUS: {response.status}")
            body = await response.text()
            print(f"BODY: {body[:500]}")
        except Exception as e:
            print(f"Submit falhou: {e}")
            print("Tentando login via fetch direto (Caminho B)...")

            # Pega o token do DOM e faz POST direto
            result = await page.evaluate('''async () => {
                const token = (document.querySelector("textarea[name='g-recaptcha-response']") || {}).value || "";
                if (!token) return {error: "Token nao encontrado no DOM"};

                const resp = await fetch("https://api-agencia.amazonasenergia.com/api/autenticacao/login", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "X-Recaptcha-Token": token,
                        "Origin": "https://agencia.amazonasenergia.com"
                    },
                    body: JSON.stringify({
                        CPF_CNPJ: "97185892287",
                        SENHA: "Iria571991",
                        TOKEN: token
                    })
                });
                const data = await resp.text();
                return {status: resp.status, body: data.substring(0, 500)};
            }''')
            print(f"FETCH RESULT: {result}")

        await browser.close()

asyncio.run(test())
