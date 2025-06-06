import logging
import os
from dotenv import load_dotenv
import concurrent.futures # 병렬 처리를 위해 추가
import time # 요청 간 간격 조절을 위해 추가
import math # 배치 수 계산을 위해 추가

from api_caller import fetch_property_list
from html_parser import parse_property_details
from gemini_analyzer import analyze_property_with_gemini
from gemini_reanalyzer import reanalyze_property_batch, REANALYSIS_BATCH_SIZE # 수정된 함수 및 배치 크기 임포트
from excel_writer import save_to_excel

# 로그 설정
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')

# 환경 변수 로드 (.env 파일 사용)
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 광화문 좌표 (예시)
GWANGHWAMUN_COORDINATES = (37.5759, 126.9780)

# 병렬 HTML 파싱 시 최대 작업자 수
MAX_WORKERS_HTML_PARSING = 25 # 동시에 요청할 스레드 수를 25로 조정 (서버 부하 고려)

# 초기 분석 배치 처리 설정
INITIAL_ANALYSIS_BATCH_SIZE = 60  # 한 번에 처리할 매물 수 (기존 BATCH_SIZE 이름 변경)
BATCH_PAUSE_SEC = 0.5  # 배치 사이의 대기 시간을 0.5초로 단축

# 재평가 배치 크기 증가
REANALYSIS_BATCH_SIZE = 100  # 재평가 배치 크기를 100으로 줄여서 API 제한 준수

def process_single_property(api_property_info, gemini_api_key, gwanghwamun_coords):
    """단일 매물에 대한 모든 처리(HTML 파싱, Gemini 분석)를 실행합니다."""
    try:
        # 매물 ID 추출
        hidx = api_property_info.get('hidx')
        if not hidx:
            logging.warning("매물 ID(hidx)가 없는 항목이 있습니다.")
            return None
        
        logging.info(f"매물 처리 시작: hidx={hidx}")
        
        # HTML 상세 페이지 파싱
        parsed_details = parse_property_details(hidx)
        if not parsed_details:
            logging.warning(f"매물 상세 페이지 파싱 실패: hidx={hidx}")
            parsed_details = {}
        
        # API 응답 + HTML 파싱 결과 병합
        combined_data = {**api_property_info, **parsed_details}
        
        # Gemini 분석
        if gemini_api_key:
            analyzed_data = analyze_property_with_gemini(combined_data, gemini_api_key, gwanghwamun_coords)
            if analyzed_data:
                return analyzed_data
            else:
                logging.warning(f"Gemini 분석 실패: hidx={hidx}")
                return combined_data
        else:
            logging.warning("Gemini API 키가 없습니다. 분석을 건너뜁니다.")
            return combined_data
    
    except Exception as e:
        logging.error(f"매물 처리 중 오류 발생: {e}")
        return api_property_info  # 최소한 API 정보는 반환
    
def process_property_batch(properties_batch_data, gemini_api_key, gwanghwamun_coords):
    """초기 분석을 위한 배치 단위 매물 처리."""
    processed_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_HTML_PARSING) as executor:
        hidx_to_property = {prop.get('hidx'): prop for prop in properties_batch_data if prop.get('hidx')}
        futures_html = {executor.submit(parse_property_details, hidx): hidx for hidx in hidx_to_property.keys()}
        
        parsed_details_map = {}
        for future in concurrent.futures.as_completed(futures_html):
            try:
                hidx = futures_html[future]
                result = future.result()
                parsed_details_map[hidx] = result if result else {}
            except Exception as e:
                logging.error(f"HTML 파싱 중 오류: {e} (hidx={futures_html[future]})")
                parsed_details_map[futures_html[future]] = {}
        
        futures_analysis = []
        for hidx, property_info in hidx_to_property.items():
            combined_data = {**property_info, **parsed_details_map.get(hidx, {})}
            if gemini_api_key:
                future = executor.submit(analyze_property_with_gemini, combined_data, gemini_api_key, gwanghwamun_coords)
                futures_analysis.append((future, combined_data))
            else:
                processed_results.append(combined_data)
        
        for i, (future, combined_data_fallback) in enumerate(futures_analysis):
            try:
                result = future.result()
                if result:
                    if 'images' in result and isinstance(result['images'], dict) and 'S' in result['images']:
                        result['images_S_length'] = len(result['images']['S'])
                    processed_results.append(result)
                    logging.info(f"Gemini 초기 분석 완료 ({i+1}/{len(futures_analysis)}): hidx={result.get('hidx')}")
                else:
                    processed_results.append(combined_data_fallback)
                    logging.warning(f"Gemini 초기 분석 결과 없음: hidx={combined_data_fallback.get('hidx')}")
            except Exception as e:
                processed_results.append(combined_data_fallback)
                logging.error(f"Gemini 초기 분석 중 오류: {e} (hidx={combined_data_fallback.get('hidx')})")
    return processed_results

def main():
    """메인 프로그램 실행 함수"""
    logging.info("피터팬 매물 분석 프로그램 시작")
    
    all_properties_from_api = [] # API에서 가져온 원본 리스트 이름 변경
    total_pages = 55 # 최대 페이지 수
    max_items_to_fetch = 1200 # 전체 매물(약 1100개) 가져오기 위해 충분히 큰 값으로 설정

    for page in range(1, total_pages + 1):
        if len(all_properties_from_api) >= max_items_to_fetch:
            logging.info(f"최대 {max_items_to_fetch}개 매물까지 조회 완료.")
            break
        logging.info(f"페이지 {page}/{total_pages} 조회 중...")
        # 페이지당 20개씩 가져오도록 설정
        api_response = fetch_property_list(page_index=page, page_size=20) 
        
        if "error" in api_response: 
            logging.error(f"API 요청 실패 (페이지 {page}): {api_response['error']}"); 
            continue
        
        page_properties = []
        if 'houses' in api_response:
            for category in api_response['houses'].keys():
                if category in api_response['houses'] and 'image' in api_response['houses'][category]:
                    page_properties.extend(api_response['houses'][category]['image'])
        
        if not page_properties: 
            logging.warning(f"페이지 {page}에서 조회된 매물이 없습니다."); 
            if page > 1: break  # 더 가져올게 없으면 중단
            continue
        
        all_properties_from_api.extend(page_properties)
        logging.info(f"페이지 {page}에서 {len(page_properties)}개 매물 조회됨 (누적: {len(all_properties_from_api)})")

    if not all_properties_from_api: logging.error("조회된 매물이 없습니다."); return
    logging.info(f"총 {len(all_properties_from_api)}개 매물 API로부터 조회 완료.")

    if not GEMINI_API_KEY: logging.warning("Gemini API 키가 없습니다. 분석/재평가 없이 진행됩니다.")

    # 초기 분석 (기존 로직 활용)
    initially_analyzed_properties = []
    logging.info(f"매물을 {INITIAL_ANALYSIS_BATCH_SIZE}개 단위로 초기 분석 배치 처리합니다.")
    for i in range(0, len(all_properties_from_api), INITIAL_ANALYSIS_BATCH_SIZE):
        batch_start_idx = i
        batch_end_idx = min(i + INITIAL_ANALYSIS_BATCH_SIZE, len(all_properties_from_api))
        current_batch_data = all_properties_from_api[batch_start_idx:batch_end_idx]
        
        logging.info(f"초기 분석 배치 처리 ({batch_start_idx+1}-{batch_end_idx}/{len(all_properties_from_api)})...")
        batch_results = process_property_batch(current_batch_data, GEMINI_API_KEY, GWANGHWAMUN_COORDINATES)
        initially_analyzed_properties.extend(batch_results)
        
        if batch_end_idx < len(all_properties_from_api):
            logging.info(f"{BATCH_PAUSE_SEC}초 대기 후 다음 초기 분석 배치...")
            time.sleep(BATCH_PAUSE_SEC)

    if not initially_analyzed_properties: logging.error("초기 분석된 매물이 없습니다."); return
    logging.info(f"성공적으로 초기 분석된 매물 수: {len(initially_analyzed_properties)}")

    # 재평가 전에 초기 분석 결과를 엑셀 파일로 저장
    def get_score_for_sort(prop):
        score_val = prop.get('total_score', 0)
        try: return int(score_val) if isinstance(score_val, (str, int, float)) else 0
        except ValueError: return 0

    # 재평가 전에 초기 분석 결과 정렬 및 엑셀 저장
    try:
        sorted_initially_analyzed = sorted(initially_analyzed_properties, key=get_score_for_sort, reverse=True)
        logging.info("초기 분석 매물을 총점 기준으로 내림차순 정렬했습니다.")
    except Exception as e:
        sorted_initially_analyzed = initially_analyzed_properties 
        logging.error(f"초기 분석 총점 기준 정렬 중 오류: {e}. 정렬되지 않은 결과 사용.")
    
    for i, prop in enumerate(sorted_initially_analyzed):
        prop['rank'] = i + 1
    
    initial_output_file = "peterpanz_initial_analysis.xlsx"
    save_to_excel(sorted_initially_analyzed, initial_output_file)
    logging.info(f"초기 분석 결과를 '{initial_output_file}' 파일로 저장했습니다.")
    logging.info(f"초기 분석 완료! 총 {len(sorted_initially_analyzed)}개 매물이 분석되었습니다.")

    # --- 재평가 단계 시작 (배치 처리) ---
    final_reanalyzed_properties = []
    if GEMINI_API_KEY and initially_analyzed_properties:
        logging.info(f"초기 분석 완료. 매물을 {REANALYSIS_BATCH_SIZE}개 단위로 전체 재평가를 시작합니다...")
        logging.info(f"재평가는 초기 분석된 모든 매물({len(initially_analyzed_properties)}개)에 대해 수행됩니다.")
        
        num_reanalysis_batches = math.ceil(len(initially_analyzed_properties) / REANALYSIS_BATCH_SIZE)
        
        for batch_idx in range(num_reanalysis_batches):
            reanalysis_batch_start_idx = batch_idx * REANALYSIS_BATCH_SIZE
            reanalysis_batch_end_idx = min((batch_idx + 1) * REANALYSIS_BATCH_SIZE, len(initially_analyzed_properties))
            current_reanalysis_batch_data = initially_analyzed_properties[reanalysis_batch_start_idx:reanalysis_batch_end_idx]
            
            if not current_reanalysis_batch_data: # 혹시 모를 빈 배치 방지
                logging.warning(f"재평가 배치 {batch_idx+1}/{num_reanalysis_batches} 데이터가 비어있어 건너뜁니다.")
                continue

            logging.info(f"재평가 배치 {batch_idx+1}/{num_reanalysis_batches} 처리 중 ({len(current_reanalysis_batch_data)}개 매물)...")
            
            # 각 배치 재평가
            reanalyzed_batch_result = reanalyze_property_batch(
                current_reanalysis_batch_data, 
                GEMINI_API_KEY, 
                batch_number=str(batch_idx+1),
                total_batches=str(num_reanalysis_batches)
            )
            
            if reanalyzed_batch_result:
                final_reanalyzed_properties.extend(reanalyzed_batch_result)
                logging.info(f"재평가 배치 {batch_idx+1}/{num_reanalysis_batches} 완료. {len(reanalyzed_batch_result)}개 결과 추가됨.")
            else:
                logging.warning(f"재평가 배치 {batch_idx+1}/{num_reanalysis_batches}에서 결과를 받지 못했습니다. 해당 배치 원본 데이터 사용.")
                final_reanalyzed_properties.extend(current_reanalysis_batch_data)
            
            # 다음 재평가 배치 전 지연 (API 속도 제한 및 부하 분산)
            if batch_idx < num_reanalysis_batches - 1:
                reanalyzer_batch_pause_sec = 15  # 재평가 배치 간 간격을 15초로 대폭 증가
                logging.info(f"{reanalyzer_batch_pause_sec}초 대기 후 다음 재평가 배치 처리...")
                time.sleep(reanalyzer_batch_pause_sec)
        
        # 재평가 후 누락된 매물이 있는지 확인하고 원본으로 채우기
        reanalyzed_hidxs = {str(prop.get('hidx')) for prop in final_reanalyzed_properties if prop.get('hidx')}
        initial_hidxs = {str(prop.get('hidx')) for prop in initially_analyzed_properties if prop.get('hidx')}
        missing_hidxs = initial_hidxs - reanalyzed_hidxs
        
        if missing_hidxs:
            logging.warning(f"재평가 과정에서 {len(missing_hidxs)}개 매물이 누락되었습니다. 해당 매물은 초기 분석 결과를 사용합니다.")
            for prop in initially_analyzed_properties:
                if str(prop.get('hidx')) in missing_hidxs:
                    # 누락 정보 추가
                    prop['ai_reanalysis_error'] = prop.get('ai_reanalysis_error', "") + "; 최종 재평가 결과에서 누락됨"
                    prop['reanalysis_comment'] = prop.get('reanalysis_comment', "") + "; 최종 재평가 결과에서 누락되어 초기 분석 데이터 사용"
                    final_reanalyzed_properties.append(prop)  # 누락된 원본 추가
        
        logging.info(f"전체 매물 재평가 완료. 최종 매물 수: {len(final_reanalyzed_properties)}")
        properties_for_excel = final_reanalyzed_properties
    else:
        if not GEMINI_API_KEY: logging.warning("API 키 없어 재평가 건너뜀.")
        if not initially_analyzed_properties: logging.warning("초기 분석된 매물 없어 재평가 불가.")
        properties_for_excel = initially_analyzed_properties
        logging.info("재평가 없이 초기 분석 결과를 최종 결과로 사용합니다.")
    # --- 재평가 단계 종료 ---

    # 재평가 후 데이터 정렬 및 최종 엑셀 파일 저장
    if not properties_for_excel:
        logging.error("최종 처리된 매물이 없습니다.")
        return

    # 백분율 점수 기준으로 정렬 (재평가가 수행된 경우)
    def get_weighted_percentile_for_sort(prop):
        percentile_score = prop.get('weighted_percentile_score')
        if percentile_score is not None:
            return float(percentile_score)
        # 백분율 점수가 없는 경우 기존 총점으로 대체
        score_val = prop.get('total_score', 0)
        try: 
            return float(score_val) if isinstance(score_val, (str, int, float)) else 0.0
        except ValueError: 
            return 0.0

    try:
        # 백분율 점수가 있는 매물이 하나라도 있으면 백분율 기준으로 정렬
        has_percentile_scores = any(prop.get('weighted_percentile_score') is not None for prop in properties_for_excel)
        
        if has_percentile_scores:
            sorted_properties = sorted(properties_for_excel, key=get_weighted_percentile_for_sort, reverse=True)
            logging.info("매물을 백분율 점수 기준으로 내림차순 정렬했습니다.")
        else:
            sorted_properties = sorted(properties_for_excel, key=get_score_for_sort, reverse=True)
            logging.info("매물을 총점 기준으로 내림차순 정렬했습니다.")
    except Exception as e:
        sorted_properties = properties_for_excel 
        logging.error(f"정렬 중 오류: {e}. 정렬되지 않은 결과 사용.")
    
    for i, prop in enumerate(sorted_properties):
        prop['rank'] = i + 1
    
    final_output_file = "peterpanz_analysis_result.xlsx"
    save_to_excel(sorted_properties, final_output_file)
    
    logging.info(f"분석 결과를 '{final_output_file}' 파일로 저장했습니다.")
    logging.info("프로그램 실행 완료")

if __name__ == "__main__":
    main()
