import pandas as pd
import logging
from datetime import datetime
import os
import re

def clean_text_for_excel(text):
    """엑셀에서 문제가 될 수 있는 특수 문자를 제거하거나 변환합니다."""
    if not isinstance(text, str):
        return text
    
    # 1. 기본적인 HTML 엔티티 처리 (예: &nbsp; -> 공백)
    text = text.replace('&nbsp;', ' ')

    # 2. 줄바꿈 및 캐리지 리턴 표준화 (엑셀은 \n을 잘 처리함)
    text = text.replace('\\r\\n', '\\n').replace('\\r', '\\n')

    # 3. 엑셀에서 문제를 일으킬 수 있는 제어 문자 제거 (ASCII 0-8, 11-12, 14-31)
    # 탭(\t, ASCII 9)과 줄바꿈(\n, ASCII 10)은 유지
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)

    # 4. 매우 제한적인 이모지 제거 (정말로 필요한 경우에만, 광범위한 패턴 사용 자제)
    # 일반적인 텍스트에 영향을 주지 않도록 매우 보수적인 패턴 사용
    # 예시: 특정 유니코드 블록의 일부만 제거하거나, 알려진 문제 이모지만 타겟팅
    # 여기서는 복잡성을 줄이기 위해 이모지 제거를 최소화하거나 주석 처리합니다.
    # emoji_pattern = re.compile(
    #     "["
    #     # 필요한 경우 여기에 매우 특정한 이모지 유니코드 범위 추가
    #     "]+", 
    #     flags=re.UNICODE
    # )
    # text = emoji_pattern.sub('', text)
            
    return text

def save_to_excel(properties_data, output_file="peterpanz_analysis_result.xlsx"):
    """
    분석 결과를 엑셀 파일로 저장합니다.
    
    Args:
        properties_data (list): 매물 데이터 리스트 (각 매물은 딕셔너리 형태)
        output_file (str): 저장할 엑셀 파일 경로
    """
    if not properties_data:
        logging.error("저장할 데이터가 없습니다.")
        return False
    
    try:
        logging.info(f"총 {len(properties_data)}개 매물 정보를 엑셀 파일로 저장합니다.")
        
        # 엑셀 컬럼 매핑 설정
        column_mapping = {
            # 기본 정보
            'rank': '순위',
            'total_score': '총점 (100점)',
            'weighted_percentile_score': '종합 백분율 점수',
            'reanalysis_rounds': '재평가 실행 횟수',
            'score_convergence': '점수 수렴 여부',
            'hidx': '매물 ID',
            'detail_url': '링크',
            
            # 백분율 점수 (재평가 결과)
            'percentile_scores.total_percentile': '총점 백분율',
            'percentile_scores.location_percentile': '위치 백분율',
            'percentile_scores.building_percentile': '건물 백분율',
            'percentile_scores.convenience_percentile': '편의 백분율',
            'percentile_scores.price_percentile': '가격 백분율',
            
            # 위치 정보
            'location.address.text': '주소',
            
            # 건물 정보
            'type.building_type': '건물 유형',
            'parsed_floor': '층',
            'parsed_total_floor': '전체 층수',
            'parsed_approval_date': '사용승인일',
            
            # 가격 정보
            'price.deposit': '보증금',
            'price.maintenance_cost': '관리비',
            
            # 크기 정보
            'info.real_size': '전용면적(㎡)',
            'info.supplied_size': '공급면적(㎡)',
            'info.real_pyeong': '전용평수',
            'info.supplied_pyeong': '공급평수',
            
            # 방/욕실 정보
            'info.room_count': '방 수',
            'parsed_bathroom_count': '욕실 수',
            
            # 옵션 정보
            'parsed_options_string': '옵션',
            'images_S_length': '이미지 개수',
            
            # 일정 정보
            'info.created_at': '등록일',
            
            # 분석 결과: 위치 및 접근성 (40점)
            'location_accessibility.location_total': '위치/접근성 총점 (40점)',
            'location_accessibility.gwanghwamun_score': '광화문 접근성 (15점)',
            'location_accessibility.gwanghwamun_comment': '광화문 접근성 평가',
            'location_accessibility.amenities_score': '주변 편의시설 (15점)',
            'location_accessibility.amenities_comment': '주변 편의시설 평가',
            'location_accessibility.transportation_score': '교통 편의성 (10점)',
            'location_accessibility.transportation_comment': '교통 편의성 평가',
            
            # 분석 결과: 건물 및 시설 품질 (30점)
            'building_quality.building_total': '건물/시설 총점 (30점)',
            'building_quality.condition_score': '건물 상태 (15점)',
            'building_quality.condition_comment': '건물 상태 평가',
            'building_quality.space_score': '공간 효율성 (10점)',
            'building_quality.space_comment': '공간 효율성 평가',
            'building_quality.floor_score': '층수/향 (5점)',
            'building_quality.floor_comment': '층수/향 평가',
            
            # 분석 결과: 옵션 및 생활 편의성 (15점)
            'living_convenience.convenience_total': '생활 편의성 총점 (15점)',
            'living_convenience.appliances_score': '가전제품 (8점)',
            'living_convenience.appliances_comment': '가전제품 평가',
            'living_convenience.furniture_score': '가구/시설 (7점)',
            'living_convenience.furniture_comment': '가구/시설 평가',
            
            # 분석 결과: 가격 경쟁력 (15점)
            'price_value.price_total': '가격 경쟁력 총점 (15점)',
            'price_value.market_score': '시세 대비 가격 (10점)',
            'price_value.market_comment': '시세 대비 가격 평가',
            'price_value.extra_cost_score': '관리비/추가비용 (5점)',
            'price_value.extra_cost_comment': '관리비/추가비용 평가',
            
            # 분석 결과: 신뢰도 및 종합 의견
            'credibility.fake_possibility': '허위매물 가능성',
            'credibility.credibility_comment': '신뢰도 평가',
            'summary.pros': '장점',
            'summary.cons': '단점',
            'summary.recommendation': '추천 대상 및 종합 의견',
            
            # 기타 정보
            'parsed_user_type': '등록인 유형',
            'parsed_agent_name': '등록인 이름',
            'parsed_agent_contact': '등록인 연락처',
            'parsed_agent_office': '등록인 사무소',
            'parsed_description': '매물 설명'
        }
        
        # 데이터 변환 및 처리
        processed_data = []
        
        for property_item in properties_data:
            row_data = {}
            
            # 깊은 경로의 값을 가져오기 위한 함수
            def get_nested_value(data, path, default=None):
                """중첩된 딕셔너리에서 .을 사용한 경로로 값을 가져옵니다."""
                if not path or not data:
                    return default
                
                try:
                    parts = path.split('.')
                    temp = data
                    for part in parts:
                        if isinstance(temp, dict) and part in temp:
                            temp = temp[part]
                        else:
                            return default
                    
                    # 리스트인 경우 문자열로 변환
                    if isinstance(temp, list):
                        return ', '.join([str(item) for item in temp])
                    return temp
                except Exception as e:
                    logging.warning(f"중첩 값 추출 중 오류: {e}, 경로: {path}")
                    return default
            
            # 각 컬럼에 대한 데이터 추출
            for col_key, col_name in column_mapping.items():
                if '.' in col_key:
                    # 복잡한 경로를 가진 키는 get_nested_value 사용
                    value = get_nested_value(property_item, col_key)
                else:
                    # 단순한 키는 직접 가져오기
                    value = property_item.get(col_key)
                
                # 문자열 값 정제 - 이모지 및 특수 문자 제거
                if isinstance(value, str):
                    value = clean_text_for_excel(value)
                
                row_data[col_name] = value
            
            # 특별 처리 항목들
            # 1. 금액 관련 항목 단위 변환 (원 -> 만원)
            money_cols = ['보증금', '관리비']
            for col in money_cols:
                if col in row_data and isinstance(row_data[col], (int, float)) and row_data[col] > 0:
                    row_data[col] = int(row_data[col] / 10000)  # 만원 단위로 변환
                elif col == '관리비' and col in row_data:
                    # 관리비 특별 처리: 다양한 경우에 대한 명확한 표시
                    if row_data[col] is None or row_data[col] == 0 or row_data[col] == '':
                        row_data[col] = "정보 없음"
                    elif isinstance(row_data[col], str):
                        if any(keyword in row_data[col] for keyword in ['확인 불가', '정보 없음', '미제공', '없음']):
                            row_data[col] = "확인 불가"
                        elif row_data[col].strip() == '0' or row_data[col].strip() == '0원' or row_data[col].strip() == '0만원':
                            row_data[col] = "0만원 (확인 필요)"  # 0원인 경우 명확히 표시
                        # 숫자가 포함된 문자열인 경우 그대로 유지
                    elif isinstance(row_data[col], (int, float)) and row_data[col] == 0:
                        row_data[col] = "0만원 (확인 필요)"
            
            # 2. URL 생성
            if '매물 ID' in row_data and row_data['매물 ID']:
                row_data['링크'] = f"https://www.peterpanz.com/house/{row_data['매물 ID']}"
            
            processed_data.append(row_data)
        
        # DataFrame 생성
        df = pd.DataFrame(processed_data)
        
        # 순위 컬럼이 없으면 추가
        if '순위' not in df.columns:
            df.insert(0, '순위', range(1, len(df) + 1))
        
        # 데이터프레임 추가 처리 - 문제가 될 수 있는 열 처리
        for col in df.columns:
            if df[col].dtype == 'object':  # 문자열 컬럼만 처리
                df[col] = df[col].apply(lambda x: clean_text_for_excel(x) if isinstance(x, str) else x)
        
        # 백업 파일 생성 (원본이 손상될 경우 대비)
        backup_file = f"{os.path.splitext(output_file)[0]}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # 엑셀 파일 저장
        try:
            # 파일 경로 확인 및 디렉토리 생성
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 엑셀 파일로 저장
            writer = pd.ExcelWriter(output_file, engine='openpyxl')
            df.to_excel(writer, index=False, sheet_name='매물분석결과')
            
            # 열 너비 자동 조정
            worksheet = writer.sheets['매물분석결과']
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).apply(len).max(),  # 데이터 내용 최대 길이
                    len(str(col))  # 컬럼명 길이
                )
                # 엑셀 최대 열 너비를 초과하지 않도록 제한 (약 250 문자)
                worksheet.column_dimensions[chr(65 + idx % 26) + ('' if idx < 26 else chr(65 + idx // 26 - 1))].width = min(max_len + 2, 50)
            
            writer.close()
            logging.info(f"엑셀 파일이 '{output_file}' 경로에 성공적으로 저장되었습니다.")
            return True
        
        except Exception as e:
            logging.error(f"엑셀 파일 저장 중 오류 발생: {e}")
            
            # 오류 발생 시 백업 전략 - CSV로 저장 시도
            try:
                csv_file = f"{os.path.splitext(output_file)[0]}.csv"
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # BOM 포함 UTF-8로 저장
                logging.info(f"CSV 백업 파일이 '{csv_file}' 경로에 저장되었습니다.")
                return True
            except Exception as csv_error:
                logging.error(f"CSV 백업 저장 중 오류 발생: {csv_error}")
                return False
    
    except Exception as e:
        logging.error(f"데이터 처리 중 예기치 않은 오류 발생: {e}")
        return False

if __name__ == "__main__":
    # 테스트 코드
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 테스트용 데이터
    test_data = [
        {
            "hidx": "12345678",
            "rank": 1,
            "total_score": 32,
            "price": {"deposit": 150000000},
            "info": {"real_size": 24.5, "supplied_size": 30.2, "room_count": 1, "floor": 3, "total_floor": 5, 
                     "created_at": "2024-07-01"},
            "location": {"address": {"text": "서울시 종로구 새문안로 123"}},
            "parsed_latitude": 37.5705,
            "parsed_longitude": 126.9765,
            "type": {"building_type": "오피스텔"},
            "parsed_approval_date": "2022.02.21",
            "parsed_floor": "저층",
            "parsed_total_floor": 12,
            "parsed_bathroom_count": 1,
            "parsed_options_string": "에어컨, 냉장고, 세탁기, 전자레인지, 신발장",
            "images": {"S": ["url1", "url2", "url3"]},
            "images_S_length": 3,
            "parsed_user_type": "agent",
            "parsed_agent_name": "홍길동",
            "parsed_agent_contact": "010-1234-5678",
            "parsed_agent_office": "행복부동산",
            "parsed_description": "깨끗한 오피스텔입니다. 역세권, 신축, 풀옵션!",
            "accessibility_score": 8,
            "accessibility_comment": "광화문까지 도보 15분 거리로 접근성이 좋습니다.",
            "building_score": 9,
            "building_comment": "2018년 신축 건물로 상태가 매우 좋습니다.",
            "options_score": 8,
            "options_comment": "기본 가전제품이 잘 갖춰져 있습니다.",
            "credibility_score": 7,
            "credibility_comment": "전문 중개사를 통한 매물로 신뢰도가 있습니다.",
            "fake_possibility": "낮음",
            "fake_reason": "상세 정보가 일관적이고 중개사 정보가 명확합니다.",
            "cost_effectiveness": "적정",
            "feedback": "전반적으로 가격 대비 가치가 좋은 매물입니다."
        },
        # 필요시 추가 테스트 데이터 항목 추가
    ]
    
    # 테스트 실행
    save_to_excel(test_data, "test_output.xlsx") 