from selenium import webdriver
from selenium.webdriver.chrome.options import Options

headers = {
    'User-Agent': 'Your User-Agent String',
    'Custom-Header': 'Your Custom Header Value'
}

cookies = [

]

chrome_options = Options()

# Set custom headers
for key, value in headers.items():
    chrome_options.add_argument(f'--header="{key}:{value}"')

# Set custom cookies
for cookie in cookies:
    chrome_options.add_cookie(cookie)

driver = webdriver.Chrome(options=chrome_options)

driver.get("https://yandex.kz/maps/api/search?add_type=direct&ajax=1&csrfToken=9615d31e9ff5ece9d6625355a518963a220f6e25%3A1689223855&ctx=ZAAAAAgBEAAaKAoSCU2fHXBdO1NAEeUn1T4dnUVAEhIJJ2qf%2BrOPrz8RIq629DL%2Fpj8iBgABAgMEBSgKOABAogFIAWISbGV0b192X2dvcm9kZT10cnVlagJrep0BzcxMPaABAKgBAL0BU91iwcIBF6ncwuQD5N%2Bwp6ED%2BdKBzcMG3M%2BX%2F%2FID6gEA8gEA%2BAEAggIW0YPQvdC40LLQtdGA0YHQuNGC0LXRgooCCTE4NDEwNjE0MJICAJoCDGRlc2t0b3AtbWFwcw%3D%3D&direct_page_id=670942&experimental%5B0%5D=relev_ranking_heavy_click_maps_formula%3D0.45%3Al3_click_dc209074_exp&experimental%5B1%5D=relev_ranking_heavy_click_serp_formula%3D0.45%3Al3_click_dc209074_exp&experimental%5B2%5D=relev_ranking_heavy_relev_maps_formula%3D0.55%3Al3_dc188536&experimental%5B3%5D=relev_ranking_heavy_relev_serp_formula%3D0.55%3Al3_dc188536&experimental_business_show_exp_features%5B0%5D=only_byak_prod&experimental_experimental%5B0%5D=leto_v_gorode%3Dtrue&internal_pron%5BadvertShimmer%5D=true&internal_pron%5BallowRepeatAds%5D=true&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bauto%5D=R-I-142300-351&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bbicycle%5D=R-I-142300-355&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bcomparison%5D=R-I-142300-350&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BdetailedRoute%5D=R-I-142300-356&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bhome%5D%5Bhd%5D=R-I-142300-343&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bhome%5D%5Bsd%5D=R-I-142300-344&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bmasstransit%5D=R-I-142300-352&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BmasstransitCard%5D%5Bhd%5D=R-I-142300-347&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BmasstransitCard%5D%5Bsd%5D=R-I-142300-348&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BorgpageBottom%5D=R-I-265853-82&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BorgpageMiddle%5D=R-I-142300-349&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5BorgpageUpper%5D=R-I-265853-81&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bpedestrian%5D=R-I-142300-353&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Bscooter%5D=R-I-142300-373&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Btaxi%5D=R-I-142300-354&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Btoponym%5D=R-I-142300-357&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Btraffic%5D%5Bhd%5D=R-I-142300-345&internal_pron%5BextendConfig%5D%5Bbanner%5D%5BblockIds%5D%5Btraffic%5D%5Bsd%5D=R-I-142300-346&internal_pron%5Bflyover%5D=true&internal_pron%5BgraphicsFpsMeter%5D=true&internal_pron%5BisUgcVideoShowing%5D=true&internal_pron%5BorgPreviewInPopup%5D=true&internal_pron%5BsaveSecondPanelContext%5D=true&internal_pron%5Btile3dPing%5D=true&internal_pron%5BvectorGraphics%5D=true&lang=ru_KZ&ll=76.907474%2C43.237668&origin=maps-mouse&parent_reqid=1689224227643994-2273311033-addrs-upper-yp-29&results=25&s=3594104785&serpid=1689223855197125-3808193049-addrs-upper-yp-35&sessionId=1689223855150_471229&skip=0&snippets=masstransit%2F2.x%2Cpanoramas%2F1.x%2Cbusinessrating%2F1.x%2Cbusinessimages%2F1.x%2Cphotos%2F2.x%2Cvideos%2F1.x%2Cexperimental%2F1.x%2Csubtitle%2F1.x%2Cvisits_histogram%2F2.x%2Ctycoon_owners_personal%2F1.x%2Ctycoon_posts%2F1.x%2Crelated_adverts%2F1.x%2Crelated_adverts_1org%2F1.x%2Ccity_chains%2F1.x%2Croute_point%2F1.x%2Ctopplaces%2F1.x%2Cmetrika_snippets%2F1.x%2Cplace_summary%2F1.x%2Conline_snippets%2F1.x%2Cbuilding_info_experimental%2F1.x%2Cprovider_data%2F1.x%2Cservice_orgs_experimental%2F1.x%2Cbusiness_awards_experimental%2F1.x%2Cbusiness_filter%2F1.x%2Chistogram%2F1.x%2Cattractions%2F1.x%2Cpotential_company_owners%3Auser%2Cpin_info%2F1.x%2Clavka%2F1.x%2Cbookings%2F1.x%2Cbookings_personal%2F1.x%2Ctrust_features%2F1.x%2Cplus_offers_experimental%2F1.x%2Ctoponym_discovery%2F1.x%2Crelevant_discovery%2F1.x%2Cvisual_hints%2F1.x%2Cfuel%2F1.x%2Crealty_experimental%2F2.x%2Cmatchedobjects%2F1.x%2Cdiscovery%2F1.x%2Ctopobjects%2F1.x%2Chot_water%2F1.x%2Cneurosummary%2Cmentioned_on_site%2F1.x&spn=0.013419%2C0.011001&test-buckets=796551%2C0%2C7%3B802385%2C0%2C63%3B797607%2C0%2C10%3B745329%2C0%2C80%3B45973%2C0%2C88%3B793589%2C0%2C99%3B681844%2C0%2C51%3B663874%2C0%2C77%3B663850%2C0%2C33%3B801489%2C0%2C23%3B803791%2C0%2C33%3B794612%2C0%2C99%3B48148%2C0%2C59%3B793259%2C0%2C37&text=%D1%83%D0%BD%D0%B8%D0%B2%D0%B5%D1%80%D1%81%D0%B8%D1%82%D0%B5%D1%82&yandex_gid=162&z=15.82")


driver.quit()
