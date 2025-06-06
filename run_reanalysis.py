import os
import logging
import json
import pandas as pd
import numpy as np
import random
import time
from gemini_reanalyzer import reanalyze_property_batch, REANALYSIS_BATCH_SIZE, NUM_REANALYSIS_ROUNDS, CONVERGENCE_THRESHOLD

# 설정값들
REANALYSIS_BATCH_SIZE = 15  # 한 번에 재평가할 매물 수 (API 안정성 향상을 위해 감소)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reanalysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def process_nested_structure(row_dict):
    """엑셀에서 읽은 플랫한 구조를 중첩 구조로 변환"""
    result = {}
    
    # 기본 필드 처리
    for key, value in row_dict.items():
        # 필드가 점(.)을 포함하는 경우 중첩 구조로 변환
        if '.' in key:
            parts = key.split('.')
            current = result
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            # 기본 필드는 그대로 유지
            result[key] = value
    
    # 특정 필드 매핑
    field_mapping = {
        '매물 ID': 'hidx',
        '총점 (100점)': 'total_score',
        '주소': 'address',
        '보증금': 'deposit',
        '추천 대상 및 종합 의견': 'recommendation'
    }
    
    for old_key, new_key in field_mapping.items():
        if old_key in row_dict:
            result[new_key] = row_dict[old_key]
    
    # 특정 중첩 구조 처리 (gemini_reanalyzer.py 구조에 맞게 조정)
    # location_accessibility, building_quality 등 구조화
    nested_fields = {
        'location_accessibility': {
            '광화문 접근성 (15점)': 'gwanghwamun_score',
            '주변 편의시설 (15점)': 'amenities_score',
            '교통 편의성 (10점)': 'transportation_score',
            '위치/접근성 총점 (40점)': 'location_total'
        },
        'building_quality': {
            '건물 상태 (15점)': 'condition_score',
            '공간 효율성 (10점)': 'space_score',
            '층수/향 (5점)': 'floor_score',
            '건물/시설 총점 (30점)': 'building_total'
        },
        'living_convenience': {
            '가전제품 (8점)': 'appliances_score',
            '가구/시설 (7점)': 'furniture_score',
            '생활 편의성  총점 (15점)': 'convenience_total'
        },
        'price_value': {
            '시세 대비 가격 (10점)': 'market_score',
            '관리비/추가비용 (5점)': 'extra_cost_score',
            '가격 경쟁력 총점 (15 점)': 'price_total'
        },
        'credibility': {
            '허위매물 가능성': 'fake_possibility',
            '신뢰도 평가': 'credibility_comment'
        }
    }
    
    for category, field_map in nested_fields.items():
        if any(field in row_dict for field in field_map):
            result[category] = {}
            for excel_field, api_field in field_map.items():
                if excel_field in row_dict and row_dict[excel_field] is not None:
                    result[category][api_field] = row_dict[excel_field]
    
    # price 필드 처리
    if 'deposit' in result:
        deposit = result.pop('deposit', None)
        result['price'] = {'deposit': deposit}
        if '관리비' in row_dict:
            maintenance_cost = row_dict['관리비']
            # 관리비 특별 처리: "확인 불가", "정보 없음" 등의 경우 별도 처리
            if isinstance(maintenance_cost, str):
                if any(keyword in maintenance_cost for keyword in ['확인 불가', '정보 없음', '미제공', '없음']):
                    result['price']['maintenance_cost'] = "확인 불가"
                elif maintenance_cost.strip() == '' or maintenance_cost.strip() == '0':
                    result['price']['maintenance_cost'] = "정보 없음"
                else:
                    # 숫자 추출 시도
                    import re
                    numbers = re.findall(r'\d+', maintenance_cost)
                    if numbers:
                        result['price']['maintenance_cost'] = int(numbers[0]) * 10000  # 만원 단위를 원 단위로
                    else:
                        result['price']['maintenance_cost'] = maintenance_cost
            elif isinstance(maintenance_cost, (int, float)):
                if maintenance_cost == 0:
                    result['price']['maintenance_cost'] = "정보 없음"
                else:
                    result['price']['maintenance_cost'] = maintenance_cost
            else:
                result['price']['maintenance_cost'] = "정보 없음"
    
    # summary 필드 처리
    if 'recommendation' in result:
        recommendation = result.pop('recommendation', None)
        result['summary'] = {'recommendation': recommendation}
    
    # 초기 reanalysis_comment 추가
    result['reanalysis_comment'] = "초기 분석 결과, 재평가 대기 중"
    
    return result

def load_properties_from_excel(excel_file):
    """엑셀 파일에서 매물 데이터 로드"""
    try:
        df = pd.read_excel(excel_file)
        logging.info(f"총 {len(df)} 개의 매물 데이터를 엑셀에서 로드했습니다.")
        
        # DataFrame을 JSON 형식의 리스트로 변환
        properties_data = []
        
        for _, row in df.iterrows():
            # NaN 값을 None으로 변환 (JSON 직렬화를 위해)
            row_dict = row.where(~pd.isna(row), None).to_dict()
            
            # 숫자형 None을 빈 문자열로 변환 (필요한 경우)
            row_dict = {k: ('' if v is None else v) for k, v in row_dict.items()}
            
            # 타임스탬프 처리
            for key, value in row_dict.items():
                if isinstance(value, (pd.Timestamp, pd.Period)):
                    row_dict[key] = value.strftime('%Y-%m-%d')
            
            # 중첩 구조 처리
            property_dict = process_nested_structure(row_dict)
            
            # 필수 필드 확인
            if 'hidx' not in property_dict:
                if '매물 ID' in row_dict:
                    property_dict['hidx'] = str(row_dict['매물 ID'])
                elif 'id' in property_dict:
                    property_dict['hidx'] = str(property_dict['id'])
                else:
                    logging.warning(f"행 {_+1}에 hidx 값이 없습니다. 인덱스를 hidx로 사용합니다.")
                    property_dict['hidx'] = str(_ + 1)
            
            # hidx가 문자열인지 확인
            if 'hidx' in property_dict and not isinstance(property_dict['hidx'], str):
                property_dict['hidx'] = str(property_dict['hidx'])
                
            properties_data.append(property_dict)
            
        return properties_data
    except Exception as e:
        logging.error(f"엑셀 파일 로드 중 오류 발생: {e}")
        logging.exception("상세 예외 정보:")
        return []

def flatten_nested_dict(d, parent_key='', sep='_'):
    """중첩된 딕셔너리를 플랫한 구조로 변환"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def save_results_to_excel(reanalyzed_data, output_file):
    """재평가 결과를 엑셀 파일로 저장"""
    try:
        # 중첩 구조를 플랫하게 만들어 DataFrame으로 변환
        flattened_data = [flatten_nested_dict(item) for item in reanalyzed_data]
        df = pd.DataFrame(flattened_data)
        
        # 결과 저장
        df.to_excel(output_file, index=False)
        logging.info(f"재평가 결과가 {output_file}에 저장되었습니다.")
    except Exception as e:
        logging.error(f"결과 저장 중 오류 발생: {e}")
        logging.exception("상세 예외 정보:")
        
        # 오류 발생 시 JSON으로 백업 저장
        try:
            with open(output_file.replace('.xlsx', '.json'), 'w', encoding='utf-8') as f:
                json.dump(reanalyzed_data, f, ensure_ascii=False, indent=2)
            logging.info("결과를 JSON으로 백업 저장했습니다.")
        except Exception as json_e:
            logging.error(f"JSON 백업 저장 중 오류 발생: {json_e}")

def calculate_weighted_average_scores(all_round_results):
    """여러 라운드 결과의 가중 평균을 계산합니다."""
    if not all_round_results:
        return []
    
    # hidx별로 결과 그룹화
    property_results = {}
    for round_idx, round_result in enumerate(all_round_results):
        weight = (round_idx + 1) / len(all_round_results)  # 최신 라운드에 더 높은 가중치
        
        for prop in round_result:
            hidx = str(prop.get('hidx'))
            if hidx not in property_results:
                property_results[hidx] = {
                    'scores': [],
                    'weights': [],
                    'properties': []
                }
            
            property_results[hidx]['scores'].append(prop.get('total_score', 0))
            property_results[hidx]['weights'].append(weight)
            property_results[hidx]['properties'].append(prop)
    
    # 가중 평균 계산
    final_results = []
    convergence_stats = {'converged': 0, 'total': 0, 'avg_variance': 0}
    total_variance = 0
    
    for hidx, data in property_results.items():
        if not data['scores']:
            continue
            
        # 가중 평균 점수 계산
        weighted_score = np.average(data['scores'], weights=data['weights'])
        
        # 점수 분산 계산 (수렴도 측정)
        variance = np.var(data['scores']) if len(data['scores']) > 1 else 0
        total_variance += variance
        
        # 수렴 여부 판단
        is_converged = variance <= CONVERGENCE_THRESHOLD
        if is_converged:
            convergence_stats['converged'] += 1
        
        convergence_stats['total'] += 1
        
        # 최신 속성 데이터 사용하되 점수는 가중 평균 적용
        final_prop = data['properties'][-1].copy()
        final_prop['total_score'] = int(round(weighted_score))
        final_prop['score_variance'] = round(variance, 2)
        final_prop['score_rounds'] = data['scores']
        final_prop['is_converged'] = is_converged
        final_prop['reanalysis_comment'] = f"다중 라운드 재평가 완료 (라운드: {len(data['scores'])}, 분산: {variance:.2f})"
        
        final_results.append(final_prop)
    
    # 수렴 통계 계산
    convergence_stats['avg_variance'] = total_variance / max(convergence_stats['total'], 1)
    convergence_stats['convergence_rate'] = convergence_stats['converged'] / max(convergence_stats['total'], 1) * 100
    
    logging.info(f"점수 수렴 통계: {convergence_stats['converged']}/{convergence_stats['total']} 매물 수렴 "
                f"(수렴률: {convergence_stats['convergence_rate']:.1f}%, 평균 분산: {convergence_stats['avg_variance']:.2f})")
    
    return final_results

def main():
    # 환경 변수에서 API 키 가져오기
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logging.error("GEMINI_API_KEY 환경 변수가 설정되지 않았습니다.")
        return
    
    # API 키에서 개행문자 및 공백 제거
    api_key = api_key.strip()
    
    # 입력 파일 및 출력 파일 설정
    input_excel = "peterpanz_initial_analysis.xlsx"
    output_excel = "peterpanz_reanalysis_results.xlsx"
    
    # 엑셀에서 데이터 로드
    properties_data = load_properties_from_excel(input_excel)
    if not properties_data:
        logging.error("매물 데이터를 로드할 수 없습니다.")
        return
    
    # 전체 매물 처리
    logging.info(f"전체 {len(properties_data)}개 매물을 처리합니다.")
    
    # 데이터 샘플 로깅 (디버깅용)
    if properties_data:
        logging.info(f"첫 번째 매물 데이터 샘플: {json.dumps(properties_data[0], ensure_ascii=False)[:500]}...")
        
        # hidx가 있는지 확인
        hidx_count = sum(1 for prop in properties_data if 'hidx' in prop and prop['hidx'])
        logging.info(f"총 {len(properties_data)}개 중 {hidx_count}개 매물에 hidx 있음")
    
    # 다중 라운드 재평가 실행
    all_round_results = []
    
    for round_num in range(1, NUM_REANALYSIS_ROUNDS + 1):
        logging.info(f"\n=== 재평가 라운드 {round_num}/{NUM_REANALYSIS_ROUNDS} 시작 ===")
        
        # 매물 순서 랜덤 셔플
        current_properties = properties_data.copy()
        random.shuffle(current_properties)
        logging.info(f"라운드 {round_num}: 매물 순서를 랜덤하게 셔플했습니다.")
        
        # 배치 크기로 데이터 분할
        total_properties = len(current_properties)
        batches = [current_properties[i:i + REANALYSIS_BATCH_SIZE] 
                   for i in range(0, total_properties, REANALYSIS_BATCH_SIZE)]
        
        logging.info(f"라운드 {round_num}: 총 {total_properties}개 매물을 {len(batches)}개 배치로 처리합니다.")
        
        # 각 배치 재평가 및 결과 수집
        round_results = []
        
        for i, batch in enumerate(batches):
            batch_number = i + 1
            logging.info(f"라운드 {round_num} - 배치 {batch_number}/{len(batches)} 처리 중 ({len(batch)}개 매물)")
            
            reanalyzed_batch = reanalyze_property_batch(
                batch, 
                api_key, 
                batch_number=f"{round_num}-{batch_number}", 
                total_batches=f"{round_num}-{len(batches)}"
            )
            
            round_results.extend(reanalyzed_batch)
            logging.info(f"라운드 {round_num} - 배치 {batch_number} 완료. 현재 라운드 {len(round_results)}개 매물 처리됨.")
            
            # 배치 간 지연
            if i < len(batches) - 1:  # 마지막 배치가 아닌 경우
                time.sleep(1)
        
        all_round_results.append(round_results)
        logging.info(f"라운드 {round_num} 완료. {len(round_results)}개 매물 처리됨.")
        
        # 라운드 간 지연
        if round_num < NUM_REANALYSIS_ROUNDS:
            time.sleep(2)
    
    # 가중 평균 점수 계산
    logging.info(f"\n=== 다중 라운드 결과 통합 중 ===")
    final_results = calculate_weighted_average_scores(all_round_results)
    
    # 결과 저장
    save_results_to_excel(final_results, output_excel)
    logging.info(f"모든 재평가 완료. 총 {len(final_results)}개 매물 처리됨.")
    logging.info(f"결과가 {output_excel}에 저장되었습니다.")

if __name__ == "__main__":
    main() 