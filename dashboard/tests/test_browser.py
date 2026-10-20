import asyncio
import json

import pytest
from playwright.async_api import async_playwright, expect
from dashboard.tests.test_app import dashboard


@pytest.mark.asyncio
async def test_browser_controls_benchmarks_mobile_and_keyboard_against_fakes(tmp_path):
    async with dashboard(tmp_path) as (client,headers,app,state,path):
        base=str(client.base_url).rstrip('/')
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True,args=['--disable-gpu'])
            page=await browser.new_page(viewport={'width':1360,'height':1000},reduced_motion='reduce')
            errors=[]
            page.on('pageerror',lambda error:errors.append(str(error)))
            await page.goto(base,wait_until='domcontentloaded')
            await page.wait_for_selector('.backend-card')
            await page.keyboard.press('Tab')
            assert await page.locator('.skip').evaluate('e=>e===document.activeElement')
            assert state['requests']==0 and app.state.test_counters['requests']==0
            await page.get_by_role('link',name='Rolling windows',exact=True).click()
            await page.wait_for_selector('.window-card')
            await page.get_by_role('link',name='Admin',exact=True).click()
            await page.wait_for_selector('[data-admin-backend="b"]')
            assert 'hidden-live-secret' not in await page.locator('#draft-config').input_value()
            await page.locator('[data-admin-backend="b"] .draining-input').check()
            await page.locator('#admin-confirm').check()
            await page.get_by_role('button',name='Apply admission changes',exact=True).click()
            await expect(page.locator('#admin-message')).to_contain_text('saved atomically')
            assert json.loads(path.read_text())['backends'][1]['draining']
            assert state['requests']==0
            await page.get_by_role('link',name='Benchmarks',exact=True).click()
            await page.locator('#bench-target').select_option('isolated')
            await expect(page.locator('#bench-backend')).to_be_disabled()
            await expect(page.locator('#bench-backend option:checked')).to_have_text('Direct · selected test engine')
            await page.locator('#bench-model').select_option('model-a')
            await page.locator('#bench-concurrency').select_option('6')
            await page.locator('#bench-requests').fill('6')
            await page.locator('#bench-tokens').fill('16')
            await page.locator('#bench-confirm').check()
            await page.get_by_role('button',name='Run benchmark',exact=True).click()
            await expect(page.locator('#bench-state')).to_have_text('complete',timeout=10000)
            assert state['requests']==0 and app.state.test_counters['requests']==6
            assert await page.locator('#bench-export a').count()==2
            await page.locator('#bench-prompt').fill('hold')
            await page.locator('#bench-confirm').check()
            await page.get_by_role('button',name='Run benchmark',exact=True).click()
            for _ in range(100):
                if app.state.test_counters['active']==6:break
                await asyncio.sleep(.02)
            assert app.state.test_counters['active']==6
            await page.get_by_role('button',name='Cancel active job',exact=True).click()
            await expect(page.locator('#bench-state')).to_have_text('cancelled')
            for _ in range(100):
                if app.state.test_counters['active']==0:break
                await asyncio.sleep(.02)
            assert app.state.test_counters['active']==0
            await page.set_viewport_size({'width':390,'height':844})
            for tab in ['Live monitor','Rolling windows','Benchmarks','Admin']:
                await page.get_by_role('link',name=tab,exact=True).click()
                await page.wait_for_timeout(100)
                assert not await page.evaluate('document.documentElement.scrollWidth>innerWidth'),tab
            assert await page.locator('#reload-config').evaluate('e=>getComputedStyle(e).transitionDuration')=='0s'
            assert errors==[]
            await browser.close()
