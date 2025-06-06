# 피터팬 매물 분석 프로그램

이 프로그램은 피터팬(peterpanz.com) 사이트의 부동산 매물 정보를 수집, 분석하여 Excel 파일로 저장합니다.
광화문 접근성, 건물 상태, 옵션 등을 종합적으로 분석하여 매물의 가치를 평가합니다.

## 주요 기능

1. 피터팬 API를 통한 매물 정보 수집
2. 매물 상세 페이지 HTML 파싱으로 추가 정보 수집
3. Google Gemini API를 이용한 다각도 분석 및 점수화
   - `thinkingBudget` 설정을 통해 분석 상세도 조절 가능
   - 광화문 접근성 평가
   - 건물 상태 및 신축 여부 평가
   - 옵션 및 편의시설 평가
   - 매물 신뢰도 및 허위매물 가능성 평가
4. 분석 결과를 엑셀 파일로 저장

## 설치 방법

1. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

2. Google AI API 키 설정:
`.env` 파일에 Google AI API 키를 설정합니다:
```
GEMINI_API_KEY="YOUR_GOOGLE_AI_API_KEY_HERE"
```

## 사용 방법

1. 프로그램 실행:
```bash
python main.py
```

2. 결과 확인:
프로그램이 실행되면 `peterpanz_analysis_result.xlsx` 파일로 분석 결과가 저장됩니다.

## 프로그램 구성

- `main.py`: 프로그램의 메인 실행 파일
- `api_caller.py`: 피터팬 API 요청 처리
- `html_parser.py`: 매물 상세 페이지 파싱
- `gemini_analyzer.py`: Google Gemini API를 이용한 매물 분석 (기존 `deepseek_analyzer.py`에서 변경)
- `excel_writer.py`: 분석 결과를 엑셀 파일로 저장
- `requirements.txt`: 필요한 라이브러리 목록
- `.env`: API 키 등 환경 설정 (gitignore에 추가 권장)

## 주의사항

- 매물 정보는 실시간으로 변경될 수 있으므로, 분석 결과는 참고용으로만 사용하세요.
- 과도한 API 요청은 서버 부하를 일으킬 수 있으므로 적절한 시간 간격을 두고 사용하세요.
- Google Gemini API 사용에는 비용이 발생할 수 있으니 API 제한 사항 및 가격 정책을 확인하세요.

## 커스터마이징

- `api_caller.py`의 `params` 값을 수정하여 필터링 조건을 변경할 수 있습니다.
- `gemini_analyzer.py`의 프롬프트를 수정하여 분석 기준을 변경할 수 있고, `GEMINI_MODEL` 및 `thinking_budget` 값을 조절하여 분석 성능과 비용을 관리할 수 있습니다.
- `excel_writer.py`의 `column_mapping`을 수정하여 엑셀 출력 항목을 변경할 수 있습니다.
- `main.py`의 `GWANGHWAMUN_COORDINATES` 값을 수정하여 다른 기준점과의 거리를 계산할 수 있습니다.

## 프로젝트 구조

```
estimate-analyze/
├── main.py               # 메인 실행 파일
├── api_caller.py         # 피터팬 API 호출 모듈
├── html_parser.py        # 매물 상세 페이지 HTML 파서 모듈
├── gemini_analyzer.py    # Google Gemini API 연동 및 분석 모듈
├── excel_writer.py       # Excel 파일 저장 모듈
├── .env                  # 환경 변수 설정 파일 (API 키 등)
├── requirements.txt      # Python 라이브러리 의존성 파일
└── README.md             # 프로그램 설명 및 사용법
```

## 설정 방법

1.  **Python 설치:** Python 3.8 이상 버전이 설치되어 있어야 합니다.
2.  **저장소 복제 (Clone Repository):**
    ```bash
    git clone <저장소_URL>
    cd estimate-analyze
    ```
3.  **가상환경 생성 및 활성화 (권장):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    # venv\Scripts\activate    # Windows
    ```
4.  **필수 라이브러리 설치:**
    ```bash
    pip install -r requirements.txt
    ```
5.  **환경 변수 설정 (.env 파일 생성):**
    프로젝트 루트 디렉토리(이 `README.md` 파일이 있는 위치)에 `.env`라는 이름의 파일을 생성하고, 아래 내용을 작성합니다. `YOUR_GOOGLE_AI_API_KEY_HERE` 부분을 실제 발급받은 Google AI API 키로 교체해야 합니다.

    ```env
    GEMINI_API_KEY="YOUR_GOOGLE_AI_API_KEY_HERE"
    ```

    **주의:** `.env` 파일은 민감한 정보를 포함하므로, Git 버전 관리에서 제외하는 것이 일반적입니다. (`.gitignore` 파일에 `.env`를 추가하세요).

## 실행 방법

프로그램을 실행하려면 프로젝트 루트 디렉토리에서 다음 명령어를 입력합니다:

```bash
python main.py
```

실행이 완료되면, 프로그램이 실행된 디렉토리에 `부동산_매물_분석_결과_YYYYMMDD_HHMMSS.xlsx` 형식의 파일 이름으로 분석 결과가 저장됩니다.

## 주요 모듈 설명

*   **`main.py`**: 전체 프로그램의 실행 흐름을 제어합니다. 데이터 수집, 분석, 저장 과정을 총괄합니다.
*   **`api_caller.py`**: `requests` 라이브러리를 사용하여 피터팬 API에 매물 리스트를 요청하고 응답을 받아옵니다. cURL을 Python 코드로 변환한 로직이 포함되어 있으며, 헤더와 파라미터를 설정합니다.
    *   **주의사항**: API의 `x-identifier-id`, `order_id` 등의 값은 동적으로 변경될 수 있습니다. 이 값들은 환경변수로 설정하여 사용하며, 실제 사용 시 API 정책을 확인하고 필요시 업데이트 로직을 추가해야 할 수 있습니다. `pageSize` 또한 API 서버의 제한을 확인해야 합니다.
*   **`html_parser.py`**: `requests`와 `BeautifulSoup4`를 사용하여 개별 매물의 상세 HTML 페이지에서 추가 정보를 추출합니다.
    *   **주의사항**: 웹사이트의 HTML 구조는 자주 변경될 수 있습니다. 만약 프로그램 실행 중 데이터가 제대로 파싱되지 않는다면, 이 파일 내의 CSS 선택자를 실제 웹사이트 구조에 맞게 수정해야 합니다. (브라우저 개발자 도구 활용)
*   **`gemini_analyzer.py`**: Google Gemini API를 호출하여 각 매물에 대한 상세 분석(접근성, 건물 상태, 신뢰도 등)을 수행하고 점수를 부여합니다.
    *   `geopy` 라이브러리를 사용하여 좌표 간 직선거리를 계산합니다.
    *   `thinking_budget` 파라미터를 사용하여 모델의 사고 토큰 수를 조절할 수 있습니다.
    *   **주의사항**: 효과적인 분석을 위해서는 Gemini API에 전달하는 프롬프트의 내용이 매우 중요합니다. 필요에 따라 프롬프트를 수정하여 분석의 질을 높일 수 있습니다.
*   **`excel_writer.py`**: `pandas` 라이브러리를 사용하여 수집 및 분석된 모든 데이터를 취합하고, 지정된 컬럼 형식에 맞춰 Excel 파일로 저장합니다. 최종 결과는 '총점' 기준으로 정렬됩니다.
    *   **주의사항**: 엑셀 컬럼명과 매칭되는 데이터 키는 실제 API 응답 및 파싱 결과의 데이터 구조에 따라 정확히 일치해야 합니다. `column_mapping` 변수를 확인하고 수정해야 할 수 있습니다.

## 오류 처리 및 로깅

*   각 모듈은 API 요청 실패, HTML 파싱 오류, 파일 저장 오류 등 다양한 예외 상황에 대한 기본적인 오류 처리 로직을 포함하고 있습니다.
*   `logging` 모듈을 사용하여 프로그램 실행 과정 및 오류 발생 시 관련 정보를 로그로 남깁니다.

## 향후 개선 사항 (TODO)

*   피터팬 API의 동적 헤더/파라미터 값 자동 업데이트 로직 구현
*   `html_parser.py`의 CSS 선택자 안정성 강화 (더 일반적인 선택자 사용 또는 정기적인 업데이트 필요)
*   `gemini_analyzer.py`의 프롬프트 최적화 및 다양한 모델/설정값 테스트
*   GUI (Graphical User Interface) 추가 (예: PyQt, Tkinter)
*   더 많은 데이터 소스 추가 (예: 다른 부동산 플랫폼)
*   필터링 조건 다양화 및 사용자 입력 처리 
