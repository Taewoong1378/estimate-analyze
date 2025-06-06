import requests
from bs4 import BeautifulSoup
import logging
import re
import json

DETAIL_PAGE_BASE_URL = "https://www.peterpanz.com/house/{hidx}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
}

def _extract_apt_info_json(soup):
    """HTML 내부의 <script> 태그에서 aptInfo JSON 데이터를 추출합니다."""
    try:
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string and "var aptInfo = {" in script.string:
                # 'var aptInfo = ' 부분을 제거하고, 세미콜론까지 잘라냄
                json_str = script.string.split("var aptInfo = ", 1)[1]
                json_str = json_str.rsplit("};", 1)[0] + "}" # 끝의 세미콜론과 불필요한 부분 제거
                apt_info = json.loads(json_str)
                logging.info("aptInfo JSON 데이터 추출 성공")
                return apt_info
    except Exception as e:
        logging.warning(f"aptInfo JSON 추출 또는 파싱 실패: {e}")
    return None

def _extract_meta_tags(soup):
    """HTML의 meta 태그에서 위경도와 기타 메타 데이터를 추출합니다."""
    meta_data = {}
    
    # 위도, 경도 정보 (og:latitude, og:longitude)
    latitude_meta = soup.find('meta', property='og:latitude')
    longitude_meta = soup.find('meta', property='og:longitude')
    
    if latitude_meta and latitude_meta.get('content'):
        try:
            meta_data['latitude'] = float(latitude_meta['content'])
            logging.info(f"  위도 (HTML meta): {meta_data['latitude']}")
        except (ValueError, TypeError):
            logging.warning(f"위도 변환 실패: {latitude_meta['content']}")
    
    if longitude_meta and longitude_meta.get('content'):
        try:
            meta_data['longitude'] = float(longitude_meta['content'])
            logging.info(f"  경도 (HTML meta): {meta_data['longitude']}")
        except (ValueError, TypeError):
            logging.warning(f"경도 변환 실패: {longitude_meta['content']}")
    
    # 기타 메타 데이터 추출 (title, description 등)
    title_meta = soup.find('meta', property='og:title')
    desc_meta = soup.find('meta', property='og:description')
    
    if title_meta and title_meta.get('content'):
        meta_data['title'] = title_meta['content']
    
    if desc_meta and desc_meta.get('content'):
        meta_data['description'] = desc_meta['content']
    
    return meta_data

def _extract_agent_info(soup, apt_info=None, property_hidx=None):
    """매물 등록자 정보(중개사 또는 직거래 판매자)를 추출합니다."""
    agent_info = {
        'name': None,
        'contact': None,
        'office': None,
        'user_type': None
    }
    
    # 1. aptInfo에서 정보 추출 (우선순위 1)
    if apt_info:
        # 사용자 유형 확인 (agent: 중개사, user: 일반 사용자/직거래)
        user_type_from_apt = apt_info.get('user_type')
        if user_type_from_apt == 'agent':
            agent_info['user_type'] = '중개사'
        elif user_type_from_apt == 'user':
            agent_info['user_type'] = '세입자'
        
        if agent_info['user_type'] == '중개사':
            # 중개사 정보 (agent_name, agent_contact, company_name 등이 있을 수 있음)
            for key in ['agent_name', 'agent_contact', 'company_name', 'phone']:
                if apt_info.get(key):
                    if 'name' in key:
                        agent_info['name'] = apt_info[key]
                    elif 'contact' in key or 'phone' in key:
                        agent_info['contact'] = apt_info[key]
                    elif 'company' in key or 'office' in key:
                        agent_info['office'] = apt_info[key]
        else: # '세입자' 또는 user_type_from_apt가 None일 경우 포함
            # 직거래 판매자 정보
            agent_info['name'] = apt_info.get('user_name', apt_info.get('author_name'))
            agent_info['contact'] = apt_info.get('user_phone', apt_info.get('phone'))
            
            # aptInfo 구조 내 다른 필드도 확인
            if agent_info['name'] is None and apt_info.get('author'):
                auth_data = apt_info.get('author')
                if isinstance(auth_data, dict):
                    agent_info['name'] = auth_data.get('name')
    
    # 2. HTML 측면 영역(Sidebar)에서 중개사/판매자 정보 추출 (우선순위 2 - aptInfo에 없을 경우)
    if agent_info['name'] is None or agent_info['contact'] is None:
        try:
            # 판매자 정보 영역
            seller_info_div = soup.select_one('.info-section.section-4')
            if seller_info_div:
                # 이름
                name_elem = seller_info_div.select_one('.profile-info strong')
                if name_elem:
                    agent_info['name'] = name_elem.text.strip()
                    logging.info(f"  직거래 판매자 이름 (HTML Sidebar): {agent_info['name']}")
                
                # 유형 (임대인, 임차인 등)
                type_elem = seller_info_div.select_one('.profile-info em')
                if type_elem:
                    agent_info['user_detail'] = type_elem.text.strip()
                    logging.info(f"  직거래 판매자 구분 (HTML Sidebar): {agent_info['user_detail']}")
                    
                    # 유형이 있다면 일반적으로 직거래(세입자)임
                    if agent_info['user_type'] is None:
                        agent_info['user_type'] = '세입자'
        
        except Exception as e:
            logging.warning(f"HTML에서 판매자 정보 추출 중 오류: {e}")
    
    # 3. 중개사 정보 전용 영역에서 추출 (우선순위 3)
    # user_type이 명시적으로 '중개사'이거나, 아직 결정되지 않았고, 이름이나 사무실 정보가 없을 때
    if (agent_info['user_type'] == '중개사' or agent_info['user_type'] is None) and \
       (agent_info['name'] is None or agent_info['office'] is None):
        try:
            # 중개사 정보 영역 (제공된 HTML 구조 기반)
            agent_section = soup.select_one('div > p.agency-name') 
            if agent_section: # p.agency-name 태그의 부모 div를 agent_section으로 간주
                agent_section_container = agent_section.parent

                # 사무소명
                office_name_elem = agent_section_container.select_one('p.agency-name')
                if office_name_elem:
                    agent_info['office'] = office_name_elem.text.strip()
                    logging.info(f"  중개사무소명 (HTML) 찾음: {agent_info['office']}")

                # 대표자 및 대표번호
                agency_info_ul = agent_section_container.select_one('.agency-info ul')
                if agency_info_ul:
                    list_items = agency_info_ul.select('li')
                    for item in list_items:
                        th_span = item.select_one('span.th')
                        td_span = item.select_one('span.td')
                        if th_span and td_span:
                            th_text = th_span.text.strip()
                            td_text = td_span.text.strip()
                            if th_text == '대표자':
                                agent_info['name'] = td_text
                                logging.info(f"  중개사 대표자명 (HTML) 찾음: {agent_info['name']}")
                            elif th_text == '대표번호':
                                agent_info['contact'] = td_text
                                logging.info(f"  중개사 대표번호 (HTML) 찾음: {agent_info['contact']}")
                
                # 중개사 정보가 있으면 user_type을 '중개사'로 설정
                if agent_info['name'] or agent_info['office']:
                    agent_info['user_type'] = '중개사'
                else: # 기존 .agent-info 등 클래스 기반 탐색
                    agent_section_fallback = soup.select_one('.agent-info, .broker-info, .realtor-info')
                    if agent_section_fallback:
                        agent_name_elem = agent_section_fallback.select_one('.agent-name, .name, strong')
                        if agent_name_elem:
                            agent_info['name'] = agent_name_elem.text.strip()
                            logging.info(f"  중개사 이름 (HTML fallback) 찾음: {agent_info['name']}")
                        
                        agent_contact_elem = agent_section_fallback.select_one('.agent-contact, .contact, .phone')
                        if agent_contact_elem:
                            agent_info['contact'] = agent_contact_elem.text.strip()
                            logging.info(f"  중개사 연락처 (HTML fallback) 찾음: {agent_info['contact']}")
                        
                        agent_office_elem = agent_section_fallback.select_one('.agent-office, .office, .company')
                        if agent_office_elem:
                            agent_info['office'] = agent_office_elem.text.strip()
                            logging.info(f"  중개사무소명 (HTML fallback) 찾음: {agent_info['office']}")

                        if agent_info['name'] or agent_info['office']:
                             agent_info['user_type'] = '중개사'

            if not (agent_info['name'] or agent_info['office'] or agent_info['contact']): # 위에서 못찾았으면 fallback
                logging.warning(f"  중개사 정보 (HTML)를 새로운 구조 또는 fallback에서 찾을 수 없습니다 {f'(hidx: {property_hidx})' if property_hidx else ''}")

        except Exception as e:
            logging.warning(f"HTML에서 중개사 정보 추출 중 오류: {e}")
    
    # 사용자 유형이 아직 None이면 '정보 없음'으로 설정
    if agent_info['user_type'] is None:
        agent_info['user_type'] = '정보 없음'
        
    return agent_info

def _extract_options(soup, property_hidx=None):
    """매물의 옵션 정보를 추출합니다."""
    options = []
    
    try:
        # 옵션 정보 추출 시도 1: detail-option-table 클래스 (실제 사이트 구조에 맞춤)
        option_table = soup.select_one('.detail-option-table')
        if option_table:
            # DD 태그에 옵션 텍스트가 있음
            option_items = option_table.select('dl dd')
            for item in option_items:
                option_text = item.text.strip()
                if option_text:
                    options.append(option_text)
            logging.info(f"  옵션 {len(options)}개 추출 성공 (detail-option-table): {', '.join(options)}")
            return options
            
        # 옵션 정보 추출 시도 2: 기존 방식
        option_section = soup.select_one('.option-section, .facility-section, .additional-option')
        if option_section:
            # 옵션 항목들 추출
            option_items = option_section.select('li, .option-item')
            for item in option_items:
                option_text = item.text.strip()
                if option_text:
                    options.append(option_text)
            logging.info(f"  옵션 {len(options)}개 추출 성공 (option-section): {', '.join(options)}")
            return options
            
        # 옵션 정보 추출 시도 3: 테이블 형식
        option_table = soup.select_one('.option-table, table.options')
        if option_table:
            rows = option_table.select('tr')
            for row in rows:
                cols = row.select('td, th')
                for col in cols:
                    option_text = col.text.strip()
                    if option_text and option_text not in ["옵션", "시설", "기타"]:
                        options.append(option_text)
            
            if options:
                logging.info(f"  옵션 {len(options)}개 추출 성공 (option-table): {', '.join(options)}")
                return options
        
        # 추가적인 시도: 다른 일반적인 옵션 컨테이너 찾기
        option_containers = soup.select('.options, .facility, .amenities, .option-list, .option-items')
        for container in option_containers:
            items = container.select('li, .item, span, div')
            for item in items:
                option_text = item.text.strip()
                if option_text and len(option_text) < 50:  # 텍스트가 너무 길지 않은 경우만 추출
                    options.append(option_text)
            
            if options:
                logging.info(f"  옵션 {len(options)}개 추출 성공 (추가 컨테이너): {', '.join(options)}")
                return options
                
        logging.info(f"  옵션 (HTML) 섹션을 찾을 수 없습니다 {f'(hidx: {property_hidx})' if property_hidx else ''}")
    except Exception as e:
        logging.warning(f"옵션 정보 추출 중 오류: {e}")
    
    return options

def parse_property_details(hidx):
    """
    매물 상세 페이지(HTML)를 파싱하여 추가 정보를 추출합니다.
    
    Args:
        hidx (str): 매물 고유 ID
        
    Returns:
        dict: 파싱된 상세 정보
    """
    url = DETAIL_PAGE_BASE_URL.format(hidx=hidx)
    logging.info(f"HTML 상세 페이지 파싱 시작: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 결과를 저장할 딕셔너리
        parsed_data = {}
        
        # 1. aptInfo JSON 추출 (가장 많은 정보가 담겨있음)
        apt_info = _extract_apt_info_json(soup)
        
        # 2. meta 태그에서 위경도 및 기타 메타 데이터 추출
        meta_data = _extract_meta_tags(soup)
        if meta_data:
            if 'latitude' in meta_data:
                parsed_data['parsed_latitude'] = meta_data['latitude']
            if 'longitude' in meta_data:
                parsed_data['parsed_longitude'] = meta_data['longitude']
            if 'title' in meta_data:
                parsed_data['parsed_title'] = meta_data['title']
        
        # 상세 설명 추출 (HTML #description-text 우선)
        description_html_element = soup.select_one('div#description-text')
        final_description = None
        if description_html_element:
            lines = []
            current_line_segments = []
            # description_html_element의 자식 노드들을 직접 순회
            for content_node in description_html_element.contents:
                if isinstance(content_node, str):  # NavigableString (텍스트 노드)
                    current_line_segments.append(str(content_node))
                elif content_node.name == 'br':
                    lines.append("".join(current_line_segments))
                    current_line_segments = []
                elif hasattr(content_node, 'get_text'):  # 다른 태그 (e.g., <span class="emoji">)
                    # 태그 내부 텍스트 추출 시 ZWJ 등 유니코드 문자 보존
                    current_line_segments.append(content_node.get_text(separator=''))

            if current_line_segments:  # 마지막 줄 처리
                lines.append("".join(current_line_segments))

            description_text_raw = "\n".join(lines)

            # 후속 공백 및 줄바꿈 처리
            # 1. 유니코드 ZWSP(\u200b) 및 기타 일반 공백들을 단일 스페이스로 변환. ZWJ(\u200d)는 건드리지 않음.
            processed_text = re.sub(r'[\s\u200b]+', ' ', description_text_raw)
            # 2. 여러 줄바꿈 및 줄바꿈 주변의 공백 정리
            processed_text = re.sub(r' *\n *', '\n', processed_text)
            # 3. 문자열 양 끝의 공백 및 줄바꿈 최종 제거
            final_description = processed_text.strip()
            
            logging.info(f"  매물 설명 (HTML #description-text, contents) 일부: {final_description[:200]}...")
        
        # HTML #description-text에 내용이 없거나, 해당 요소가 없는 경우 aptInfo 사용
        if not final_description and apt_info:
            description_apt = apt_info.get('description') or apt_info.get('content')
            if not description_apt and apt_info.get('info') and isinstance(apt_info['info'], dict):
                description_apt = apt_info['info'].get('description') or apt_info['info'].get('subject')
            
            if description_apt:
                # apt_info에서 가져온 설명도 HTML 포함 가능성 있으므로 정리
                temp_soup = BeautifulSoup(description_apt, 'html.parser')
                for br_tag in temp_soup.find_all('br'):
                    br_tag.replace_with('\n')
                description_apt_cleaned = temp_soup.get_text(strip=True)
                final_description = re.sub(r'[\s\u200b]+', ' ', description_apt_cleaned)
                final_description = re.sub(r'( ?\n ?)+', '\n', final_description).strip()
                logging.info(f"  매물 설명 (aptInfo) 일부: {final_description[:200]}...")

        if final_description:
            parsed_data['parsed_description'] = final_description
        elif meta_data and 'description' in meta_data: # 최후의 보루로 meta 태그 description 사용
            parsed_data['parsed_description'] = meta_data['description'].strip()
            logging.info(f"  매물 설명 (meta tag) 일부: {meta_data['description'][:200]}...")

        # 3. aptInfo에서 데이터 추출 (상세 설명은 위에서 처리)
        if apt_info:
            # 상세 설명은 위에서 이미 처리했으므로 aptInfo의 description을 다시 덮어쓰지 않음
            pass # description = apt_info.get('description') or apt_info.get('content') 등은 제거
            
            # 욕실 수
            bathroom_count = apt_info.get('bathroom_count')
            if bathroom_count is None and apt_info.get('info') and isinstance(apt_info['info'], dict):
                bathroom_count = apt_info['info'].get('bathroom_count')
            
            if bathroom_count:
                parsed_data['parsed_bathroom_count'] = bathroom_count
                logging.info(f"  욕실 수 (aptInfo): {bathroom_count}")
            
            # 사용자 타입 및 중개사/판매자 정보
            user_type_from_apt = apt_info.get('user_type')
            if user_type_from_apt == 'agent':
                parsed_data['parsed_user_type'] = '중개사'
            elif user_type_from_apt == 'user':
                parsed_data['parsed_user_type'] = '세입자'
            
            # 직거래 정보
            if parsed_data.get('parsed_user_type') == '세입자' or not parsed_data.get('parsed_user_type'):
                seller_name = apt_info.get('user_name')
                seller_contact = apt_info.get('phone') or apt_info.get('user_phone')
                
                if seller_name:
                    parsed_data['parsed_agent_name'] = seller_name
                    logging.info(f"  직거래 판매자 이름 (aptInfo): {seller_name}")
                
                if seller_contact:
                    parsed_data['parsed_agent_contact'] = seller_contact
                    logging.info(f"  직거래 판매자 연락처 (aptInfo): {seller_contact}")
            
            # 중개사 정보
            agent_info = _extract_agent_info(soup, apt_info, hidx)
            
            if agent_info['user_type']:
                parsed_data['parsed_user_type'] = agent_info['user_type']
            
            if agent_info['name'] and ('parsed_agent_name' not in parsed_data or not parsed_data['parsed_agent_name']):
                parsed_data['parsed_agent_name'] = agent_info['name']
            
            if agent_info['contact'] and ('parsed_agent_contact' not in parsed_data or not parsed_data['parsed_agent_contact']):
                parsed_data['parsed_agent_contact'] = agent_info['contact']
            
            if agent_info['office']:
                parsed_data['parsed_agent_office'] = agent_info['office']
        
        # 4. HTML 파싱으로 추가 정보 추출 (aptInfo에 없는 정보)
        
        # 사용승인일 정보 추출 (새로운 로직)
        try:
            approval_date_th = soup.find(lambda tag: tag.name == 'div' and tag.get('class') == ['detail-table-th'] and "사용승인일" in tag.text.strip())
            if approval_date_th:
                approval_date_td = approval_date_th.find_next_sibling('div', class_='detail-table-td')
                if approval_date_td:
                    approval_date_text = approval_date_td.text.strip()
                    parsed_data['parsed_approval_date'] = approval_date_text
                    logging.info(f"  사용승인일 (HTML): {approval_date_text}")
            else:
                logging.info(f"  사용승인일 정보 (HTML)를 찾을 수 없습니다.")
        except Exception as e:
            logging.warning(f"HTML에서 사용승인일 정보 추출 중 오류: {e}")

        # 층/전체 층수 정보 추출 (새로운 로직)
        try:
            floor_info_th = soup.find(lambda tag: tag.name == 'div' and tag.get('class') == ['detail-table-th'] and "해당층/전체층" in tag.text)
            if floor_info_th:
                floor_info_td = floor_info_th.find_next_sibling('div', class_='detail-table-td')
                if floor_info_td:
                    floor_text = floor_info_td.text.strip()
                    if '/' in floor_text:
                        current_floor_str, total_floor_str = floor_text.split('/', 1)
                        parsed_data['parsed_floor'] = current_floor_str.strip()
                        # "층" 문자 제거 및 숫자만 추출 시도
                        total_floor_numeric = re.search(r'\d+', total_floor_str)
                        if total_floor_numeric:
                            parsed_data['parsed_total_floor'] = int(total_floor_numeric.group(0))
                        else:
                            parsed_data['parsed_total_floor'] = total_floor_str.strip()
                        logging.info(f"  층/전체층 (HTML): {parsed_data['parsed_floor']}/{parsed_data['parsed_total_floor']}")
                    else:
                        # 형식에 맞지 않는 경우 일단 현재 층 정보로만 기록
                        parsed_data['parsed_floor'] = floor_text
                        logging.info(f"  층 정보만 (HTML): {parsed_data['parsed_floor']}")
            else:
                logging.info(f"  층/전체층 정보 (HTML)를 찾을 수 없습니다.")
        except Exception as e:
            logging.warning(f"HTML에서 층/전체층 정보 추출 중 오류: {e}")
            
        # 옵션 정보 추출
        options = _extract_options(soup, hidx)
        if options:
            parsed_data['parsed_options'] = options
            parsed_data['parsed_options_string'] = ', '.join(options)
        
        logging.info(f"HTML 상세 페이지 파싱 완료: {url}")
        return parsed_data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"HTML 파싱 요청 중 오류 발생: {e}")
    except Exception as e:
        logging.error(f"HTML 파싱 중 예기치 않은 오류 발생: {e} (URL: {url})")
    
    return {}

if __name__ == "__main__":
    # 테스트 실행
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 테스트용 hidx (실제 존재하는 매물 ID로 변경 필요)
    test_hidx = "17124606"  # 예시 ID
    
    test_result = parse_property_details(test_hidx)
    if test_result:
        print("\n--- 파싱 결과 ---")
        for key, value in test_result.items():
            print(f"{key}: {value}")
    else:
        print(f"매물 ID {test_hidx}의 상세 정보 파싱 실패") 