import logging
import json # JSON 파싱을 위해 추가
import requests # geopy 거리 계산 실패 시 대체 경로 등에 사용될 수 있으므로 유지
from geopy.distance import geodesic
import time # RateLimit 대비용
import random # 무작위 지연을 위해 추가
import re # JSON 추출을 위해 추가
from google import genai
from google.genai import types

# 사용할 Gemini 모델명
GEMINI_MODEL = "gemini-2.5-flash-preview-05-20"

# 광화문까지의 대중교통 시간 (분) - 예시 값
GWANGHWAMUN_TRANSIT_TIME_MINUTES = 60

# API 호출 속도 제한 관련 설정
API_MAX_CALLS_PER_MINUTE = 200  # 분당 최대 API 호출 수를 200개로 증가
API_MIN_DELAY_SECONDS = 0.3  # 연속 API 호출 사이 최소 지연 시간을 0.3초로 단축

# 마지막 API 호출 시간을 추적하기 위한 전역 변수
last_api_call_time = 0

# JSON 파싱 오류 수정 기능 추가
def fix_json_string(json_str):
    """일반적인 JSON 파싱 오류를 자동으로 수정합니다."""
    if not json_str:
        return json_str
    
    # 1. 후행 쉼표 제거 (객체 및 배열)
    fixed_json = re.sub(r',\s*}', '}', json_str)
    fixed_json = re.sub(r',\s*]', ']', fixed_json)
    
    # 2. 속성 이름 쌍따옴표 확인 - 작은따옴표를 큰따옴표로
    fixed_json = re.sub(r'(\w+):\s*', r'"\1": ', fixed_json) 
    
    # 3. 작은따옴표로 묶인 문자열을 큰따옴표로 변환
    # (단, 큰따옴표 내부의 작은따옴표는 보존해야 하므로 복잡한 처리 필요)
    # 간단한 접근법으로 시작 - 모든 값부분만 조정
    fixed_json = re.sub(r':\s*\'([^\']*?)\'', r': "\1"', fixed_json)
    
    return fixed_json

def extract_and_parse_json(response_text, hidx=None):
    """응답 텍스트에서 JSON 블록을 추출하고 파싱합니다. 오류 발생 시 수정을 시도합니다."""
    if not response_text:
        return None
    
    # 1. JSON 블록 추출
    json_match = re.search(r"```json\s*([\s\S]*?)\s*```", response_text)
    if not json_match:
        logging.error(f"JSON 블록을 찾을 수 없습니다 (hidx={hidx}).")
        return None
    
    json_str = json_match.group(1).strip()
    if not json_str:
        logging.error(f"추출된 JSON 문자열이 비어있습니다 (hidx={hidx}).")
        return None
    
    # 2. 기본 JSON 파싱 시도
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logging.warning(f"JSON 파싱 오류 발생 (hidx={hidx}): {e}. 자동 수정 시도 중...")
        error_msg = str(e)
        
        # 3. 오류 수정 시도
        fixed_json = fix_json_string(json_str)
        if fixed_json != json_str:
            logging.info(f"JSON 문자열을 수정했습니다 (hidx={hidx}).")
            
            try:
                return json.loads(fixed_json)
            except json.JSONDecodeError as fix_e:
                logging.error(f"수정된 JSON도 파싱 실패 (hidx={hidx}): {fix_e}")
                
                # 4. 디버그를 위한 로깅 
                logging.debug(f"원본 JSON (hidx={hidx}):\n{json_str}\n")
                logging.debug(f"수정된 JSON (hidx={hidx}):\n{fixed_json}\n")
                
                # 5. 마지막 시도: 아주 단순하게 후행 쉼표만 제거 
                if "Trailing comma" in str(fix_e) or "trailing comma" in str(fix_e):
                    very_simple_fix = json_str.replace(",}", "}").replace(",\n}", "\n}")
                    try:
                        return json.loads(very_simple_fix)
                    except:
                        pass
        
        return None

def get_distance_to_gwanghwamun(lat, lon, gwanghwamun_coords):
    """주어진 위도, 경도와 광화문 좌표 사이의 직선 거리를 계산합니다."""
    if lat is None or lon is None:
        logging.warning("위도 또는 경도 정보가 없어 광화문까지의 거리를 계산할 수 없습니다.")
        return None
    
    if isinstance(lat, str):
        try:
            lat = float(lat)
        except (ValueError, TypeError):
            logging.warning(f"위도 값({lat})을 숫자로 변환할 수 없습니다.")
            return None
    
    if isinstance(lon, str):
        try:
            lon = float(lon)
        except (ValueError, TypeError):
            logging.warning(f"경도 값({lon})을 숫자로 변환할 수 없습니다.")
            return None
    
    try:
        property_coords = (lat, lon)
        distance = geodesic(property_coords, gwanghwamun_coords).kilometers
        logging.info(f"광화문까지의 직선 거리: {distance:.2f} km (좌표: {lat}, {lon})")
        return distance
    except Exception as e:
        logging.error(f"거리 계산 중 오류 발생: {e}")
        return None

def analyze_property_with_gemini(property_data, api_key, gwanghwamun_coords):
    """
    Google Gemini API를 사용하여 매물 데이터를 분석하고 점수를 매깁니다.
    
    Args:
        property_data (dict): 매물의 기본 정보와 HTML 파싱 결과를 포함한 데이터
        api_key (str): Google AI API 키
        gwanghwamun_coords (tuple): 광화문의 위도, 경도 튜플 (예: (37.5759, 126.9780))
        
    Returns:
        dict: 분석 결과와 기존 매물 정보를 병합한 데이터
    """
    if not property_data:
        logging.warning("분석할 매물 데이터가 없습니다.")
        return None
    
    if not api_key:
        logging.error("Google AI API 키가 없습니다. GEMINI_API_KEY 환경변수를 확인하세요.")
        property_data['ai_analysis_error'] = "Google AI API 키 없음"
        return property_data 
    
    hidx = property_data.get('hidx')
    if not hidx:
        logging.warning("매물 ID(hidx)가 없습니다.")
        return property_data
    
    logging.info(f"Gemini API 분석 시작: hidx={hidx} (모델: {GEMINI_MODEL})")
    
    try:
        client = genai.Client(api_key=api_key)
        
        system_instruction = "당신은 한국의 부동산 시장에 정통한 전문가입니다. 제공된 매물 정보를 객관적으로 분석하고 점수를 매깁니다."
        
        generation_config = types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=3000,
            thinking_config=types.ThinkingConfig(thinking_budget=1024)
        )
    except Exception as e:
        logging.error(f"Gemini API 클라이언트 초기화 중 오류 발생: {e}")
        property_data['ai_analysis_error'] = f"Gemini API 클라이언트 초기화 실패: {e}"
        return property_data

    lat = property_data.get('parsed_latitude')
    lon = property_data.get('parsed_longitude')
    if lat is None or lon is None:
        location = property_data.get('location', {})
        if location:
            lat = location.get('latitude') or location.get('lat') or location.get('y') or (location.get('address', {}) or {}).get('latitude')
            lon = location.get('longitude') or location.get('lon') or location.get('lng') or location.get('x') or (location.get('address', {}) or {}).get('longitude')
    
    distance_to_gwanghwamun = get_distance_to_gwanghwamun(lat, lon, gwanghwamun_coords)
    
    address_from_api = None
    location_data = property_data.get('location', {})
    if location_data and isinstance(location_data, dict):
        address_obj = location_data.get('address', {})
        if address_obj and isinstance(address_obj, dict):
            address_from_api = address_obj.get('text')

    if not address_from_api:
        for field in ['location_text', 'address', 'addr', 'location.address.text']:
            parts = field.split('.')
            temp_data = property_data
            valid_path = True
            for part in parts:
                if isinstance(temp_data, dict) and part in temp_data:
                    temp_data = temp_data[part]
                else:
                    valid_path = False
                    break
            if valid_path and isinstance(temp_data, str):
                address_from_api = temp_data
                break
    address = address_from_api or '주소 정보 없음'

    description = property_data.get('parsed_description') or \
                  (property_data.get('info') or {}).get('subject') or \
                  property_data.get('description') or '상세 설명 없음'

    price_data = property_data.get('price', {})
    if not isinstance(price_data, dict): price_data = {}
    price_deposit_val = price_data.get('deposit') or (property_data.get('info', {}) or {}).get('deposit')
    price_info_str = f"보증금: {price_deposit_val / 10000 if isinstance(price_deposit_val, (int, float)) else price_deposit_val}만원"
    
    # 관리비 정보 추가
    maintenance_cost_val = price_data.get('maintenance_cost') or price_data.get('monthly_rent')
    if maintenance_cost_val:
        if isinstance(maintenance_cost_val, str) and any(keyword in maintenance_cost_val for keyword in ['확인 불가', '정보 없음', '미제공']):
            price_info_str += f", 관리비: {maintenance_cost_val}"
        elif isinstance(maintenance_cost_val, (int, float)) and maintenance_cost_val > 0:
            price_info_str += f", 관리비: {maintenance_cost_val / 10000}만원"
        else:
            price_info_str += ", 관리비: 정보 없음"
    else:
        price_info_str += ", 관리비: 정보 없음"

    approval_date_val = property_data.get('parsed_approval_date', '정보 없음')
    
    building_type_val = (property_data.get('type', {}) or {}).get('building_type', '정보 없음')
    if isinstance(building_type_val, list) and len(building_type_val) > 0: building_type_val = building_type_val[0]
    
    room_count_val = (property_data.get('info', {}) or {}).get('room_count')
    bathroom_count_val = property_data.get('parsed_bathroom_count')
    
    size_info_data = {}
    for field in ['info', 'size']:
        if field in property_data and isinstance(property_data[field], dict):
            size_info_data = property_data[field]
            break
    supplied_size_val = size_info_data.get('supplied_size')
    real_size_val = size_info_data.get('real_size')
    size_text_str = f"{supplied_size_val}㎡(공급) / {real_size_val}㎡(전용)" if supplied_size_val and real_size_val else "정보 없음"
    
    floor_val = property_data.get('parsed_floor')
    total_floor_val = property_data.get('parsed_total_floor')
    floor_info_str = f"{floor_val}/{total_floor_val}층" if floor_val and total_floor_val else (f"{floor_val}층" if floor_val else "정보 없음")
    
    options_string_val = property_data.get('parsed_options_string', '')
    
    agent_name_val = property_data.get('parsed_agent_name', '정보 없음')
    agent_office_val = property_data.get('parsed_agent_office', '')
    user_type_val = property_data.get('parsed_user_type') or (property_data.get('attribute', {}) or {}).get('userType')

    # 사용자 유형을 '중개사' 또는 '세입자'로 명확히 표시
    user_type_display = '정보 없음'
    if user_type_val == 'agent' or user_type_val == '중개사':
        user_type_display = '중개사'
    elif user_type_val == 'user' or user_type_val == '세입자':
        user_type_display = '세입자'

    prompt = f"""
당신은 부동산 전문가입니다. 아래 제공된 매물 정보를 바탕으로 상세 분석을 수행하고, 점수를 매겨주세요. 
총 100점 만점 기준으로 각 카테고리별 점수와 근거를 구체적으로 설명해주세요.

## 매물 기본 정보
- 주소: {address}
- 설명: {description}
- 가격: {price_info_str}
- 사용승인일: {approval_date_val}
- 건물 유형: {building_type_val}
- 방/욕실 수: {room_count_val}개/{bathroom_count_val}개
- 면적: {size_text_str}
- 층수: {floor_info_str}
- 옵션: {options_string_val}
- 매물 등록인: {agent_name_val} ({agent_office_val}) ({user_type_display})
"""
    if distance_to_gwanghwamun:
        prompt += f"- 광화문까지 직선거리: {distance_to_gwanghwamun:.2f}km\\n"

    prompt += """
## 분석 항목 (총 100점 만점)

1. 위치 및 접근성 (40점 만점)
   a. 광화문 접근성 (15점): 광화문까지의 직선거리 및 대중교통 이용 편의성
   b. 주변 편의시설 (15점): 마트, 병원, 공원, 상가 등 생활편의시설 접근성
   c. 교통 편의성 (10점): 지하철역, 버스정류장 접근성, 교통 연결성

2. 건물 및 시설 품질 (30점 만점)
   a. 건물 상태 및 연식 (15점): 사용승인일, 리모델링 여부, 건물 관리상태
   b. 공간 효율성 (10점): 구조, 면적 대비 활용도, 수납공간
   c. 층수 및 향 (5점): 저층/고층 여부, 일조량, 조망권

3. 옵션 및 생활 편의성 (15점 만점)
   a. 가전제품 (8점): 냉장고, 세탁기, 에어컨 등 필수 가전 보유 여부
   b. 가구 및 시설 (7점): 붙박이장, 신발장, 인테리어 품질

4. 가격 경쟁력 (15점 만점)
   a. 동일 지역 시세 대비 가격 (10점): 주변 유사 매물 대비 가격 경쟁력
   b. 관리비 및 추가비용 (5점): 관리비, 주차비 등 추가 비용 요소
      ※ 중요: 관리비 정보가 "확인 불가", "정보 없음" 또는 누락된 경우, 이는 투명성 부족으로 간주하여 점수를 낮게 부여하세요 (1-2점).
      관리비가 명확히 제시된 경우에만 적정 점수(3-5점)를 부여하세요.

## 추가 분석
1. 매물 신뢰도 평가
   - 허위매물 가능성: 낮음/보통/높음 중 하나를 선택하고 그 이유 설명
   - 매물 정보의 일관성, 상세함, 사진 제공 여부
   - 중개사/판매자 정보의 투명성

2. 종합 의견
   - 장점 요약 (3가지 이상)
   - 단점 요약 (2가지 이상)
   - 추천 대상 (어떤 사람에게 적합한지)

## 응답 형식
응답은 반드시 아래 JSON 형식으로 제공해주세요:

```json
{
  "total_score": "점수(0-100 숫자)",
  
  "location_accessibility": {
    "gwanghwamun_score": "점수(0-15 숫자)",
    "gwanghwamun_comment": "광화문 접근성에 대한 평가",
    "amenities_score": "점수(0-15 숫자)",
    "amenities_comment": "주변 편의시설 평가",
    "transportation_score": "점수(0-10 숫자)",
    "transportation_comment": "교통 편의성 평가",
    "location_total": "총합(0-40 숫자)"
  },
  
  "building_quality": {
    "condition_score": "점수(0-15 숫자)",
    "condition_comment": "건물 상태 평가",
    "space_score": "점수(0-10 숫자)",
    "space_comment": "공간 효율성 평가",
    "floor_score": "점수(0-5 숫자)",
    "floor_comment": "층수 및 향 평가",
    "building_total": "총합(0-30 숫자)"
  },
  
  "living_convenience": {
    "appliances_score": "점수(0-8 숫자)",
    "appliances_comment": "가전제품 평가",
    "furniture_score": "점수(0-7 숫자)",
    "furniture_comment": "가구 및 시설 평가",
    "convenience_total": "총합(0-15 숫자)"
  },
  
  "price_value": {
    "market_score": "점수(0-10 숫자)",
    "market_comment": "시세 대비 가격 평가",
    "extra_cost_score": "점수(0-5 숫자)",
    "extra_cost_comment": "관리비 및 추가비용 평가",
    "price_total": "총합(0-15 숫자)"
  },
  
  "credibility": {
    "fake_possibility": "낮음/보통/높음 중 하나",
    "credibility_comment": "신뢰도 평가 근거"
  },
  
  "summary": {
    "pros": ["장점1", "장점2", "장점3"],
    "cons": ["단점1", "단점2"],
    "recommendation": "추천 대상 및 종합 의견"
  }
}
```
각 점수는 반드시 배점 범위 내에서 정수로 부여해주세요. 각 카테고리의 총합은 하위 항목들의 합과 일치해야 합니다.
total_score는 모든 카테고리 점수의 합으로, 100점 만점입니다. JSON 내부의 값은 모두 문자열로 반환해주세요. 숫자인 경우에도 따옴표로 감싸주세요.
"""

    MAX_RETRY = 5  # 최대 재시도 횟수 증가
    retry_count = 0
    response_text = None
    
    # API 분석 실패 시 반환할 기본 결과
    fallback_result = {
        "total_score": "50", 
        "location_accessibility": {
            "gwanghwamun_score": "8", "gwanghwamun_comment": "API 분석 실패",
            "amenities_score": "8", "amenities_comment": "API 분석 실패",
            "transportation_score": "8", "transportation_comment": "API 분석 실패",
            "location_total": "24"
        },
        "building_quality": {
            "condition_score": "8", "condition_comment": "API 분석 실패",
            "space_score": "5", "space_comment": "API 분석 실패",
            "floor_score": "3", "floor_comment": "API 분석 실패",
            "building_total": "16"
        },
        "living_convenience": {
            "appliances_score": "4", "appliances_comment": "API 분석 실패",
            "furniture_score": "3", "furniture_comment": "API 분석 실패",
            "convenience_total": "7"
        },
        "price_value": {
            "market_score": "5", "market_comment": "API 분석 실패",
            "extra_cost_score": "3", "extra_cost_comment": "API 분석 실패",
            "price_total": "8"
        },
        "credibility": {"fake_possibility": "보통", "credibility_comment": "API 분석 실패"},
        "summary": {
            "pros": ["API 분석 실패"], "cons": ["API 분석 실패"],
            "recommendation": "API 분석 실패"
        },
        "reanalysis_comment": "개별 분석 실패로 재평가 정보 없음"
    }
    
    while retry_count < MAX_RETRY:
        try:
            logging.info(f"Gemini API 요청 시작 (모델: {GEMINI_MODEL}, 시도: {retry_count + 1})")
            
            # API 속도 제한을 위한 지연 로직
            global last_api_call_time
            current_time = time.time()
            elapsed_time = current_time - last_api_call_time
            
            # 연속 API 호출 사이에 최소한의 지연 시간 적용
            if last_api_call_time > 0 and elapsed_time < API_MIN_DELAY_SECONDS:
                wait_time = API_MIN_DELAY_SECONDS - elapsed_time
                logging.info(f"API 속도 제한 준수를 위해 {wait_time:.2f}초 대기 중...")
                time.sleep(wait_time + random.uniform(0.1, 1.0))  # 약간의 무작위성 추가
            
            # 재시도 시 지수 백오프 적용 (첫 번째 시도에는 적용 안 함)
            if retry_count > 0:
                # 지수 백오프: 2^retry_count * (10~15초) 의 지연 시간
                backoff_time = (2 ** retry_count) * (10 + random.uniform(0, 5))
                logging.info(f"재시도 #{retry_count}: {backoff_time:.2f}초 대기 중...")
                time.sleep(backoff_time)
            
            # API 호출 시간 기록
            last_api_call_time = time.time()
            
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[
                    {"role": "user", "parts": [{"text": prompt}]},
                ],
                config=generation_config
            )
            
            if response and hasattr(response, 'text'):
                response_text = response.text
                logging.info("Gemini API로부터 분석 결과를 성공적으로 받았습니다.")
                break
            else:
                logging.warning(f"Gemini API 응답이 비정상적입니다: {response}")
                retry_count += 1
        
        except Exception as e:
            error_message = str(e)
            logging.error(f"Gemini API 호출 중 오류 발생: {error_message}")
            
            # 429 에러 (할당량 초과) 처리 로직
            if "429" in error_message or "RESOURCE_EXHAUSTED" in error_message:
                retry_delay = 30  # 기본 재시도 지연 시간(초)
                
                # 에러 메시지에서 retryDelay 값 추출 시도
                retry_delay_match = re.search(r"retryDelay': '(\d+)s'", error_message)
                if retry_delay_match:
                    retry_delay = int(retry_delay_match.group(1)) + random.randint(5, 10)  # 여유 있게 몇 초 더 기다림
                
                logging.warning(f"할당량 제한에 도달했습니다. {retry_delay}초 후 재시도합니다.")
                time.sleep(retry_delay)
            else:
                # 다른 오류일 경우 기본 지수 백오프 적용
                backoff_time = (2 ** retry_count) * (5 + random.uniform(0, 5))
                time.sleep(backoff_time)
            
            retry_count += 1
            
            if retry_count >= MAX_RETRY:
                property_data['ai_analysis_error'] = f"Gemini API 호출 실패: {error_message}"
                return {**property_data, **fallback_result}
    
    analysis_result = None
    if response_text:
        try:
            # 새로운 JSON 추출 및 파싱 함수 사용
            analysis_result = extract_and_parse_json(response_text, hidx)
            
            if analysis_result:
                logging.info("Gemini API 응답 JSON 파싱 성공.")
                
                def convert_to_int_safe(value_dict, key):
                    if key in value_dict and isinstance(value_dict[key], str):
                        try:
                            value_dict[key] = int(value_dict[key])
                        except ValueError:
                            logging.warning(f"점수 변환 실패 (정수 아님): {key}={value_dict[key]}")
                
                if 'total_score' in analysis_result: convert_to_int_safe(analysis_result, 'total_score')
                for category_key in ['location_accessibility', 'building_quality', 'living_convenience', 'price_value']:
                    if category_key in analysis_result and isinstance(analysis_result[category_key], dict):
                        category_data = analysis_result[category_key]
                        for score_key in category_data:
                            if 'score' in score_key or 'total' in score_key :
                                convert_to_int_safe(category_data, score_key)
            else:
                logging.error("Gemini API 응답에서 JSON 파싱 실패.")
                property_data['ai_analysis_error'] = "JSON 파싱 실패"
                property_data['ai_analysis_raw_response'] = response_text
                return {**property_data, **fallback_result}

        except Exception as e:
            logging.error(f"Gemini API 응답 처리 중 예기치 않은 오류: {e}")
            property_data['ai_analysis_error'] = f"응답 처리 오류: {e}"
            property_data['ai_analysis_raw_response'] = response_text
            return {**property_data, **fallback_result}

    if not analysis_result:
        property_data['ai_analysis_error'] = property_data.get('ai_analysis_error', "Gemini 분석 결과 없음")
        return {**property_data, **fallback_result}

    result = {**property_data, **analysis_result}
    if 'reanalysis_comment' not in result:
        result['reanalysis_comment'] = "개별 분석 완료. 재평가 대기 중."

    logging.info(f"매물 ID: {hidx} 분석 완료. 총점: {analysis_result.get('total_score', 'N/A')}/100점")
    
    return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    test_property = {
        "hidx": "test12345",
        "info": { "subject": "Gemini 테스트용 신축 오피스텔", "supplied_size": 30.0, "real_size": 25.0, "deposit": 50000000, "room_count": 1 },
        "price": { "deposit": 50000000 },
        "type": { "building_type": "오피스텔" },
        "location": { "address": { "text": "서울시 강남구 테헤란로 123" } },
        "parsed_latitude": 37.504540, "parsed_longitude": 127.048903,
        "parsed_approval_date": "2023.01.15",
        "parsed_floor": "고층",
        "parsed_total_floor": 20,
        "parsed_bathroom_count": 1,
        "parsed_options_string": "에어컨,냉장고,세탁기,인덕션,옷장,신발장,TV",
        "parsed_agent_name": "김테스트", "parsed_user_type": "중개사"
    }
    
    import os
    test_api_key = os.getenv("GEMINI_API_KEY") 

    if not test_api_key or test_api_key == "YOUR_GOOGLE_AI_API_KEY": # API 키 확인 강화
        logging.warning("테스트용 API 키를 GEMINI_API_KEY 환경변수에 설정해주세요.")
    else:
        gwanghwamun_coords_test = (37.5759, 126.9780)
        # test_result = analyze_property_with_gemini(test_property, test_api_key, gwanghwamun_coords_test)
        # if test_result:
        #     print(json.dumps(test_result, indent=2, ensure_ascii=False))
        # else:
        #     print("테스트 분석 실패")
        logging.info("테스트 실행은 analyze_property_with_gemini 함수 호출 부분을 주석 해제하고 API 키를 GEMINI_API_KEY 환경변수에 설정해야 합니다.") 