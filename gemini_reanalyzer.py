import logging
import json
import time
import random
import re
import os
from google import genai
from google.genai import types

# 사용할 Gemini 모델명
GEMINI_MODEL_REANALYZER = "gemini-2.5-flash-preview-05-20"  # 더 빠른 모델로 변경

# API 호출 속도 제한 관련 설정
API_MAX_CALLS_PER_MINUTE_REANALYZER = 100  # 분당 호출 수를 대폭 줄임
API_MIN_DELAY_SECONDS_REANALYZER = 2.0  # API 호출 간 최소 지연 시간 (초)

# 재평가 시 한 번에 처리할 매물 수 (배치 크기)
REANALYSIS_BATCH_SIZE = 15  # 한 번에 재평가할 매물 수 (API 안정성 향상을 위해 감소)

# 다중 라운드 재평가 설정
NUM_REANALYSIS_ROUNDS = 5  # 재평가 라운드 수 (더 안정적인 수렴을 위해 증가)
CONVERGENCE_THRESHOLD = 5.0  # 점수 수렴 임계값

# 마지막 API 호출 시간을 추적하기 위한 전역 변수
last_api_call_time_reanalyzer = 0

def calculate_percentile_scores(properties_list):
    """매물들의 점수를 백분율로 변환하여 더 명확한 순위를 만듭니다."""
    if not properties_list:
        return properties_list
    
    # 각 카테고리별 점수 수집
    location_scores = []
    building_scores = []
    convenience_scores = []
    price_scores = []
    total_scores = []
    
    for prop in properties_list:
        try:
            if 'location_accessibility' in prop and isinstance(prop['location_accessibility'], dict):
                loc_total = prop['location_accessibility'].get('location_total', 0)
                location_scores.append(int(loc_total) if isinstance(loc_total, (str, int, float)) else 0)
            else:
                location_scores.append(0)
                
            if 'building_quality' in prop and isinstance(prop['building_quality'], dict):
                building_total = prop['building_quality'].get('building_total', 0)
                building_scores.append(int(building_total) if isinstance(building_total, (str, int, float)) else 0)
            else:
                building_scores.append(0)
                
            if 'living_convenience' in prop and isinstance(prop['living_convenience'], dict):
                conv_total = prop['living_convenience'].get('convenience_total', 0)
                convenience_scores.append(int(conv_total) if isinstance(conv_total, (str, int, float)) else 0)
            else:
                convenience_scores.append(0)
                
            if 'price_value' in prop and isinstance(prop['price_value'], dict):
                price_total = prop['price_value'].get('price_total', 0)
                price_scores.append(int(price_total) if isinstance(price_total, (str, int, float)) else 0)
            else:
                price_scores.append(0)
                
            total_score = prop.get('total_score', 0)
            total_scores.append(int(total_score) if isinstance(total_score, (str, int, float)) else 0)
        except:
            location_scores.append(0)
            building_scores.append(0)
            convenience_scores.append(0)
            price_scores.append(0)
            total_scores.append(0)
    
    # 각 점수별 백분율 계산
    def calculate_percentile(scores):
        sorted_scores = sorted(scores)
        percentiles = {}
        for score in set(scores):
            rank = sorted_scores.index(score) + 1
            percentile = (rank / len(sorted_scores)) * 100
            percentiles[score] = round(percentile, 1)
        return percentiles
    
    location_percentiles = calculate_percentile(location_scores)
    building_percentiles = calculate_percentile(building_scores)
    convenience_percentiles = calculate_percentile(convenience_scores)
    price_percentiles = calculate_percentile(price_scores)
    total_percentiles = calculate_percentile(total_scores)
    
    # 백분율 정보를 각 매물에 추가
    for i, prop in enumerate(properties_list):
        prop['percentile_scores'] = {
            'location_percentile': location_percentiles.get(location_scores[i], 0),
            'building_percentile': building_percentiles.get(building_scores[i], 0),
            'convenience_percentile': convenience_percentiles.get(convenience_scores[i], 0),
            'price_percentile': price_percentiles.get(price_scores[i], 0),
            'total_percentile': total_percentiles.get(total_scores[i], 0)
        }
        
        # 종합 백분율 점수 계산 (가중 평균)
        weighted_percentile = (
            prop['percentile_scores']['location_percentile'] * 0.4 +
            prop['percentile_scores']['building_percentile'] * 0.3 +
            prop['percentile_scores']['convenience_percentile'] * 0.15 +
            prop['percentile_scores']['price_percentile'] * 0.15
        )
        prop['weighted_percentile_score'] = round(weighted_percentile, 2)
    
    return properties_list

def reanalyze_property_batch(properties_batch_data, api_key, batch_number="N/A", total_batches="N/A"):
    """매물 배치를 재평가하고 백분율 기반 순위 조정을 수행합니다."""
    if not properties_batch_data:
        logging.warning(f"재평가할 매물 데이터가 없습니다 (배치 {batch_number}/{total_batches}).")
        return []

    if not api_key:
        logging.error(f"Google AI API 키가 없습니다 (배치 {batch_number}/{total_batches}).")
        # API 키가 없어도 백분율 계산은 수행
        return calculate_percentile_scores(properties_batch_data)
    
    # API 키에서 개행문자 및 공백 제거
    api_key = api_key.strip()
        
    batch_hidx_list = [str(prop.get('hidx')) for prop in properties_batch_data if prop.get('hidx') is not None]
    batch_hidx_set = set(batch_hidx_list)
    
    if not batch_hidx_list:
        logging.warning(f"재평가할 매물 데이터에 유효한 hidx가 없습니다 (배치 {batch_number}/{total_batches}).")
        return calculate_percentile_scores(properties_batch_data)

    logging.info(f"Gemini API 배치 재평가 시작 (배치 {batch_number}/{total_batches}, 모델: {GEMINI_MODEL_REANALYZER}, 매물 수: {len(properties_batch_data)})")
    logging.info(f"현재 배치 hidx 목록: {', '.join(batch_hidx_list[:20])}{'...' if len(batch_hidx_list) > 20 else ''}")

    try:
        # API 호출 지연 처리
        global last_api_call_time_reanalyzer
        current_time = time.time()
        elapsed_since_last_call = current_time - last_api_call_time_reanalyzer
        
        if elapsed_since_last_call < API_MIN_DELAY_SECONDS_REANALYZER:
            sleep_time = API_MIN_DELAY_SECONDS_REANALYZER - elapsed_since_last_call
            logging.info(f"API 속도 제한 준수를 위해 {sleep_time:.2f}초 대기 중...")
            time.sleep(sleep_time)
        
        client = genai.Client(api_key=api_key)
        
        # 더 구체적이고 명확한 프롬프트 작성 (인코딩 문제 해결)
        prompt_text = f"""
다음 {len(properties_batch_data)}개 매물을 재평가해주세요. 각 매물의 hidx는 절대 변경하지 마세요.

매물 데이터:
{json.dumps(properties_batch_data, ensure_ascii=False, indent=1)}

요구사항:
1. 모든 매물을 빠짐없이 처리하세요
2. hidx는 원본 그대로 유지하세요  
3. 총점은 0-100 사이 정수로 조정하세요
4. 반드시 JSON 배열 형태로 응답하세요

응답 형식 (예시):
```json
[
  {{
    "hidx": "원본hidx그대로",
    "total_score": 85,
    "location_accessibility": {{"location_total": 35}},
    "building_quality": {{"building_total": 25}},
    "living_convenience": {{"convenience_total": 12}},
    "price_value": {{"price_total": 13}},
    "reanalysis_comment": "재평가 완료"
  }}
]
```

처리할 hidx 목록: {', '.join(batch_hidx_list)}
"""
        
        generation_config = types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=30000,  # 응답 잘림 방지를 위해 증가
            top_p=0.9,
        )
        
        MAX_RETRY = 3
        retry_count = 0
        response_text = None
        
        while retry_count < MAX_RETRY:
            try:
                logging.info(f"Gemini API 요청 시작 (모델: {GEMINI_MODEL_REANALYZER}, 시도: {retry_count + 1})")
                
                response = client.models.generate_content(
                    model=GEMINI_MODEL_REANALYZER, 
                    contents=[{"role": "user", "parts": [{"text": prompt_text}]}],
                    config=generation_config
                )
                
                if response and hasattr(response, 'text') and response.text:
                    response_text = response.text.strip()
                    logging.info(f"Gemini API로부터 배치 재평가 결과 수신 (배치 {batch_number}/{total_batches}).")
                    logging.debug(f"API 응답 길이: {len(response_text)} 문자")
                    logging.debug(f"API 응답 시작 부분 (200자): {response_text[:200]}")
                    
                    # API 응답에서 hidx 개수 확인
                    hidx_count_in_response = len(re.findall(r'"hidx"\s*:\s*"[^"]*"', response_text))
                    logging.info(f"API 응답에서 발견된 hidx 개수: {hidx_count_in_response}, 요청한 매물 수: {len(properties_batch_data)}")
                    
                    break
                else:
                    logging.warning(f"Gemini API 응답이 비어있음 (배치 {batch_number}/{total_batches}, 시도 {retry_count + 1}).")
                    retry_count += 1
                    if retry_count < MAX_RETRY:
                        wait_time = 2 ** retry_count
                        logging.info(f"빈 응답으로 인한 재시도 전 {wait_time}초 대기...")
                        time.sleep(wait_time)
            
            except Exception as e:
                error_message = str(e)
                logging.error(f"Gemini API 호출 중 오류 (배치 {batch_number}/{total_batches}, 시도 {retry_count + 1}): {error_message}")
                retry_count += 1
                
                if retry_count < MAX_RETRY:
                    if "429" in error_message or "RESOURCE_EXHAUSTED" in error_message:
                        # 할당량 초과 시 더 긴 대기
                        wait_time = 60 + (retry_count * 30)
                        logging.warning(f"할당량 제한 (배치 {batch_number}/{total_batches}). {wait_time}초 후 재시도.")
                        time.sleep(wait_time)
                    else:
                        wait_time = 2 ** retry_count
                        logging.info(f"오류 후 재시도 전 {wait_time}초 대기...")
                        time.sleep(wait_time)
                
        last_api_call_time_reanalyzer = time.time()
        
        if not response_text:
            logging.error(f"Gemini API 응답을 받지 못함 (배치 {batch_number}/{total_batches}). 백분율 계산만 수행.")
            return calculate_percentile_scores(properties_batch_data)

        # JSON 추출 및 파싱 (강화된 로직)
        try:
            json_str = None
            reanalyzed_list = None
            
            # 1. 마크다운 JSON 블록 추출 시도
            json_match = re.search(r"```json\s*([\s\S]*?)\s*```", response_text, re.DOTALL | re.IGNORECASE)
            if json_match:
                json_str = json_match.group(1).strip()
                logging.info(f"마크다운 JSON 블록 추출 성공 (배치 {batch_number}/{total_batches})")
            
            # 2. 일반 마크다운 블록 추출 시도
            if not json_str:
                json_match = re.search(r"```\s*([\s\S]*?)\s*```", response_text, re.DOTALL)
                if json_match:
                    potential_json = json_match.group(1).strip()
                    if potential_json.startswith('[') and potential_json.endswith(']'):
                        json_str = potential_json
                        logging.info(f"일반 마크다운 블록에서 JSON 추출 (배치 {batch_number}/{total_batches})")
            
            # 3. 직접 JSON 배열 찾기 (마크다운 없음)
            if not json_str:
                json_match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0).strip()
                    logging.info(f"직접 JSON 배열 추출 (배치 {batch_number}/{total_batches})")
            
            # 4. 단일 객체 형태일 경우 배열로 변환
            if not json_str:
                json_match = re.search(r'\{\s*"hidx"\s*:\s*"([^"]+)"[^}]*\}' , response_text, re.DOTALL)
                if json_match:
                    single_obj = json_match.group(0).strip()
                    json_str = f"[{single_obj}]"
                    logging.info(f"단일 객체를 배열로 변환 (배치 {batch_number}/{total_batches})")
            
            if not json_str:
                logging.error(f"JSON 블록을 찾을 수 없음 (배치 {batch_number}/{total_batches}).")
                logging.debug(f"원본 응답 (처음 1000자): {response_text[:1000]}")
                return calculate_percentile_scores(properties_batch_data)
            
            # JSON 파싱 시도
            try:
                reanalyzed_list = json.loads(json_str)
                logging.info(f"JSON 파싱 성공 (배치 {batch_number}/{total_batches}): {len(reanalyzed_list)}개 항목")
            except json.JSONDecodeError as e:
                logging.warning(f"JSON 파싱 실패, 수동 수정 시도 (배치 {batch_number}/{total_batches}): {e}")
                
                # 5. 일반적인 JSON 오류 수정
                fixed_json = json_str
                
                # 후행 쉼표 제거
                fixed_json = re.sub(r',(\s*[}\]])', r'\1', fixed_json)
                        
                # 속성명에 따옴표 추가
                fixed_json = re.sub(r'(\w+):', r'"\1":', fixed_json)
                
                # 문자열 값 따옴표 수정
                fixed_json = re.sub(r':\s*([^",\[\]{}]+)(?=\s*[,}\]])', r': "\1"', fixed_json)
                
                # 숫자는 따옴표 제거
                fixed_json = re.sub(r':\s*"(\d+)"', r': \1', fixed_json)
                fixed_json = re.sub(r':\s*"(\d+\.\d+)"', r': \1', fixed_json)
                
                try:
                    reanalyzed_list = json.loads(fixed_json)
                    logging.info(f"수정된 JSON 파싱 성공 (배치 {batch_number}/{total_batches})")
                except json.JSONDecodeError as fix_e:
                    logging.error(f"JSON 수정 후에도 파싱 실패 (배치 {batch_number}/{total_batches}): {fix_e}")
                        
                    # 6. 마지막 시도: 정규식으로 개별 객체 추출
                    try:
                        logging.info(f"정규식으로 개별 객체 추출 시도 (배치 {batch_number}/{total_batches})")
                        individual_objects = []
                        
                        # hidx가 포함된 객체들을 개별적으로 찾기
                        object_pattern = r'\{\s*"hidx"\s*:\s*"([^"]+)"[^}]*\}'
                        matches = re.finditer(object_pattern, response_text, re.DOTALL)
                        
                        for match in matches:
                            obj_str = match.group(0)
                            try:
                                obj = json.loads(obj_str)
                                individual_objects.append(obj)
                            except:
                                # 개별 객체도 파싱 실패하면 기본값 생성
                                hidx = match.group(1)
                                individual_objects.append({
                                    "hidx": hidx,
                                    "total_score": 50,
                                    "reanalysis_comment": "JSON 파싱 실패로 기본값 사용"
                                })
                        
                        if individual_objects:
                            reanalyzed_list = individual_objects
                            logging.info(f"정규식으로 {len(individual_objects)}개 객체 추출 성공")
                        else:
                            logging.error(f"모든 JSON 파싱 시도 실패 (배치 {batch_number}/{total_batches})")
                            return calculate_percentile_scores(properties_batch_data)
                    except Exception as regex_e:
                        logging.error(f"정규식 추출도 실패 (배치 {batch_number}/{total_batches}): {regex_e}")
                        return calculate_percentile_scores(properties_batch_data)
            
            # 결과 검증 및 병합 (강화된 로직)
            initial_batch_map = {str(prop['hidx']): prop for prop in properties_batch_data if 'hidx' in prop}
            final_properties = []
            processed_hidxs = set()
            
            logging.info(f"초기 배치 매물 수: {len(initial_batch_map)}, API 응답 항목 수: {len(reanalyzed_list)}")
            
            for item in reanalyzed_list:
                hidx = str(item.get('hidx', ''))
                logging.debug(f"처리 중인 hidx: {hidx}")
                
                if hidx in batch_hidx_set and hidx not in processed_hidxs:
                    if hidx in initial_batch_map:
                        # 점수 정수 변환
                        for key in ['total_score']:
                            if key in item:
                                try:
                                    item[key] = int(float(item[key]))
                                except (ValueError, TypeError):
                                    logging.warning(f"hidx {hidx}의 {key} 값 변환 실패: {item[key]}")
                        
                        for category in ['location_accessibility', 'building_quality', 'living_convenience', 'price_value']:
                            if category in item and isinstance(item[category], dict):
                                for sub_key in item[category]:
                                    if 'total' in sub_key or 'score' in sub_key:
                                        try:
                                            item[category][sub_key] = int(float(item[category][sub_key]))
                                        except (ValueError, TypeError):
                                            logging.warning(f"hidx {hidx}의 {category}.{sub_key} 값 변환 실패: {item[category][sub_key]}")
                        
                        initial_batch_map[hidx].update(item)
                        final_properties.append(initial_batch_map[hidx])
                        processed_hidxs.add(hidx)
                        logging.debug(f"hidx {hidx} 성공적으로 처리됨")
                    else:
                        logging.warning(f"hidx {hidx}가 초기 배치에 없음")
                else:
                    if hidx in processed_hidxs:
                        logging.warning(f"hidx {hidx} 중복 처리 시도")
                    elif hidx not in batch_hidx_set:
                        logging.warning(f"hidx {hidx}가 현재 배치에 속하지 않음")
            
            # 누락된 매물들 추가
            missing_count = 0
            for hidx in batch_hidx_set:
                if hidx not in processed_hidxs and hidx in initial_batch_map:
                    prop = initial_batch_map[hidx]
                    prop['reanalysis_comment'] = "재평가 API 응답에서 누락되어 원본 데이터 유지"
                    final_properties.append(prop)
                    missing_count += 1
                    logging.warning(f"현재 배치(배치 {batch_number}/{total_batches})의 hidx '{hidx}'가 재평가 API 응답에서 누락됨. 원본 데이터 유지.")
            
            if missing_count > 0:
                logging.warning(f"총 {missing_count}개 매물이 API 응답에서 누락됨")
            
            # 백분율 점수 계산
            final_properties = calculate_percentile_scores(final_properties)
            
            logging.info(f"배치 재평가 완료 (배치 {batch_number}/{total_batches}). 최종 매물 수: {len(final_properties)}, 성공 처리: {len(processed_hidxs)}, 누락: {missing_count}")
            return final_properties
            
        except Exception as e:
            logging.error(f"응답 처리 중 오류 (배치 {batch_number}/{total_batches}): {e}")
            logging.exception("상세 예외 정보:")
            return calculate_percentile_scores(properties_batch_data)

    except Exception as e:
        logging.error(f"재평가 중 예기치 않은 오류 (배치 {batch_number}/{total_batches}): {e}")
        logging.exception("상세 예외 정보:")
        return calculate_percentile_scores(properties_batch_data)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    sample_properties = [
        {
            "hidx": "test001", "total_score": 80, 
            "location_accessibility": {"location_total": 30},
            "building_quality": {"building_total": 25},
            "living_convenience": {"convenience_total": 12},
            "price_value": {"price_total": 13}
        },
        {
            "hidx": "test002", "total_score": 75,
            "location_accessibility": {"location_total": 25},
            "building_quality": {"building_total": 20},
            "living_convenience": {"convenience_total": 15},
            "price_value": {"price_total": 15}
        }
    ]
    
    api_key_env = os.getenv("GEMINI_API_KEY")
    if api_key_env:
        result = reanalyze_property_batch(sample_properties, api_key_env, "1", "1")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        logging.warning("GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")
        result = calculate_percentile_scores(sample_properties)
        print("백분율 계산 결과:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
