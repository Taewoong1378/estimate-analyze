import requests
import logging
import json
from urllib.parse import urlencode
import os

# API 엔드포인트 기본 URL
PROPERTY_LIST_API_URL = "https://api.peterpanz.com/houses/area"

# 요청 헤더 설정 (CURL과 일치하도록 수정)
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json;charset=utf-8',
    'origin': 'https://www.peterpanz.com',
    'referer': 'https://www.peterpanz.com/',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'x-identifier-id': os.getenv('PETERPANZ_IDENTIFIER_ID', 'your_identifier_id_here'),  # 환경변수로 변경
    'x-peterpanz-os': 'web',
    'x-peterpanz-page-id': 'PAGE_UNKNOWN',
    'x-peterpanz-uidx': 'undefined',
    'x-peterpanz-version': '3.52.0'
}

def fetch_property_list(page_index=1, page_size=20):
    """
    피터팬 API에서 지정된 조건에 맞는 매물 목록을 가져옵니다.
    
    Args:
        page_index (int): 조회할 페이지 번호 (기본값: 1)
        page_size (int): 페이지당 조회할 매물 수 (기본값: 20)
    
    Returns:
        dict: 요청 성공 시 API 응답 데이터, 실패 시 오류 정보
    """
    # CURL 명령어 형식의 파라미터로 수정
    params = {
        'zoomLevel': 12,
        'center': json.dumps({
            'y': 37.566628,
            '_lat': 37.566628,
            'x': 126.978038,
            '_lng': 126.978038
        }).replace(' ', ''),
        'dong': '',
        'gungu': '',
        'filter': 'latitude:37.4495189~37.6835533||longitude:126.8736678~127.2746689||checkDeposit:100000000~200000000||roomCount_etc;["2층~5층","6층~9층","10층 이상"]||contractType;["전세"]||additional_options;["전세자금대출"]||buildingType;["원/투룸"]',
        'pageSize': page_size,
        'pageIndex': page_index,
        'order_id': os.getenv('PETERPANZ_ORDER_ID', 'your_order_id_here'),  # 환경변수로 변경
        'search': '',
        'filter_version': '5.1',
        'response_version': '5.2',
        'order_by': 'random'
    }
    
    logging.info(f"피터팬 API 요청: pageIndex={page_index}, pageSize={page_size}")
    
    try:
        # GET 방식으로 API 요청 (POST에서 GET으로 변경)
        full_url = f"{PROPERTY_LIST_API_URL}?{urlencode(params)}"
        logging.info(f"요청 URL: {full_url[:100]}...")  # URL이 너무 길면 일부만 로깅
        
        response = requests.get(
            full_url,
            headers=HEADERS,
            timeout=30
        )
        response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
        
        # 응답 JSON 파싱
        response_data = response.json()
        
        # 데이터 존재 여부 확인
        houses_data = response_data.get('houses', {})
        
        # API 응답 구조 확인 (최상위 레벨)
        logging.info(f"API 응답 최상위 키: {', '.join(response_data.keys())}")
        
        # 'houses' 내 카테고리 확인
        if houses_data:
            houses_categories = houses_data.keys()
            logging.info(f"houses 카테고리: {', '.join(houses_categories)}")
            
            # 매물 개수 확인
            houses_count = 0
            for category in houses_categories:
                if category in houses_data and 'image' in houses_data[category]:
                    houses_count += len(houses_data[category]['image'])
            
            logging.info(f"API 응답 성공: 총 {houses_count}개 매물 데이터 수신")
        else:
            logging.warning("API 응답에 houses 데이터가 없습니다.")
        
        return response_data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"API 요청 중 오류 발생: {e}")
        return {"error": str(e)}
    
    except json.JSONDecodeError as e:
        logging.error(f"API 응답 JSON 파싱 오류: {e}")
        return {"error": f"API 응답 JSON 파싱 오류: {e}"}
    
    except Exception as e:
        logging.error(f"API 호출 중 예기치 않은 오류: {e}")
        return {"error": f"API 호출 중 예기치 않은 오류: {e}"}

if __name__ == "__main__":
    # 테스트용 코드
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    result = fetch_property_list()
    if "error" not in result:
        # API 응답 구조 확인
        print("--- API 응답 구조 ---")
        top_level_keys = result.keys()
        print(f"최상위 키: {top_level_keys}")
        
        # houses 카테고리 확인
        if 'houses' in result:
            houses_categories = result['houses'].keys()
            print(f"houses 카테고리: {houses_categories}")
            
            # 매물 개수 출력
            total_count = 0
            for category in houses_categories:
                if 'image' in result['houses'][category]:
                    count = len(result['houses'][category]['image'])
                    total_count += count
                    print(f"  - {category}: {count}개 매물")
            
            print(f"총 매물 수: {total_count}개")
            
            # 첫 번째 매물 정보 샘플 출력 (있을 경우)
            for category in houses_categories:
                if 'image' in result['houses'][category] and len(result['houses'][category]['image']) > 0:
                    first_property = result['houses'][category]['image'][0]
                    print(f"\n첫 번째 {category} 매물 샘플:")
                    print(f"  hidx: {first_property.get('hidx')}")
                    print(f"  위치: {first_property.get('location', {}).get('address', {}).get('text', '위치 정보 없음')}")
                    print(f"  가격: 보증금 {first_property.get('price', {}).get('deposit', '정보 없음')}원")
                    break
    else:
        print(f"API 요청 실패: {result['error']}") 