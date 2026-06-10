# 스타트업 생존·폐업 분석과 몬테카를로 포트폴리오 최적화

> 빅데이터분석 파이널 프로젝트 — Spark 기반 대용량 전처리와 10만 회 몬테카를로 시뮬레이션을 활용한 벤처 투자 포트폴리오 분석

> 💡 **핵심 결과:** Crunchbase 스타트업 데이터를 Spark로 전처리·집계하여 국가·산업별 폐업률과 투자금액–생존율 관계를 분석하고, **10만 개 포트폴리오 시나리오**를 몬테카를로로 탐색해 위험–수익 효율 경계와 최적 자본 배분안을 도출했습니다.

---

## 1. 문제 정의 (Problem Statement)

벤처 투자자는 한정된 자본을 여러 산업·국가의 스타트업에 배분할 때 **"어디에 투자해야 폐업 위험은 낮고 성공(인수·IPO) 가능성은 높은가"** 를 판단해야 합니다. 그러나 공개 데이터에는 펀드 수익률(IRR)이 없으므로, 본 분석은 성과를 다음으로 정의합니다.

- **폐업률:** `status == closed` 비율 (낮을수록 안정적)
- **성공률:** `status ∈ {acquired, ipo}` 비율 (성공 엑시트)
- **생존율:** `status != closed` 비율

이를 통해 국가·산업 단위의 위험을 측정하고, 이를 입력으로 몬테카를로 시뮬레이션을 돌려 **위험 대비 기대 성공률이 가장 높은 자본 배분**을 탐색합니다.

---

## 2. 데이터 및 방법론 (Data & Methods)

| 항목 | 내용 |
|------|------|
| 데이터 출처 | Kaggle: `yanmaksi/big-startup-secsees-fail-dataset-from-crunchbase` |
| 파일 | `big_startup_secsees_dataset.csv` |
| 처리 엔진 | **Apache Spark (PySpark)** — 분산 전처리·집계 |
| 주요 필드 | `funding_total_usd`, `country_code`, `category_list`, `status` |
| 파생 변수 | `is_closed`, `is_survived`, `is_success` |
| 시뮬레이션 | **몬테카를로 10만 회** (numpy) — 포트폴리오 배분 시나리오 탐색 |
| 시각화 | Matplotlib · Seaborn (효율 경계 산점도, 자본 배분 누적 막대) |

### 전처리 핵심 (에러 원천 차단)
`funding_total_usd`는 문자열에 통화 기호·콤마가 섞여 있어, **① 숫자/소수점만 추출 → ② 정규식으로 완전한 숫자 형태인지 재검증 → ③ 유효할 때만 `Double` 캐스팅, 아니면 `Null` 처리 → ④ 결측 행 제거** 순으로 정제했습니다. Spark의 ANSI 모드를 끄고 형변환 실패를 안전하게 처리했습니다.

---

## 3. 핵심 결과 (Key Findings)

### 3-1. 국가별 폐업률 (표본 50개 이상)
| 구분 | 결과 |
|------|------|
| 폐업률 최고 Top 10 | `[본인 결과 입력]` |
| 폐업률 최저 Bottom 10 | `[본인 결과 입력]` |

### 3-2. 산업 카테고리별 폐업률 (표본 50개 이상)
| 구분 | 결과 |
|------|------|
| 폐업률 최고 Top 10 | `[본인 결과 입력]` |
| 폐업률 최저 Bottom 10 | `[본인 결과 입력]` |

### 3-3. 투자금액과 생존율의 상관관계
`funding_total_usd`와 `is_survived` 간 피어슨 상관계수 = **`[본인 결과 입력]`**

### 3-4. 카테고리별 포트폴리오 지표 (표본 100개 이상)
성공률·평균 필요 자본을 기준으로 시뮬레이션 입력 카테고리 5개를 선정했습니다.

| 카테고리 | 성공률 | 평균 필요 자본(M$) |
|----------|--------|--------------------|
| Biotech & Health | 89.09% | 117.48 |
| Semiconductors | 30.25% | 64.28 |
| Web Hosting | 29.66% | 31.77 |
| Public Relations | 28.31% | 14.80 |
| Security | 25.13% | 18.78 |

### 3-5. 몬테카를로 포트폴리오 시뮬레이션
- **총 예산 500M\$**, **10만 개** 무작위 배분 시나리오 생성
- 각 시나리오의 기대 성공률·리스크 점수·투자 가능 기업 수 산출
- 리스크 점수 = `(100 − 기대성공률) + 집중 페널티`(투자 기업 수가 20개 미만일 때 가중)
- 세 가지 대표 시나리오 도출:

| 시나리오 | 예상 성공률 | 리스크 점수 | 투자 기업 수 |
|----------|-------------|-------------|--------------|
| 🔥 최대 수익 (Max Return) | `[본인 결과 입력]` | `[본인 결과 입력]` | `[본인 결과 입력]` |
| 🛡️ 최저 리스크 (Min Risk) | `[본인 결과 입력]` | `[본인 결과 입력]` | `[본인 결과 입력]` |
| 💣 최고 리스크 (Max Risk) | `[본인 결과 입력]` | `[본인 결과 입력]` | `[본인 결과 입력]` |

> 💡 **인사이트:** 위험–수익 산점도에서 좌상단(낮은 리스크 · 높은 성공률)에 가까운 배분이 효율적이며, 단일 산업 몰빵보다 분산 배분이 집중 페널티를 낮춰 안정적인 결과를 보입니다.

---

## 4. 분석 코드 스니펫 (Code Snippet)

### Spark 전처리 — 안전한 형변환
```python
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

cleaned = F.regexp_replace(F.col("funding_total_usd"), "[^0-9.]", "")
is_valid = cleaned.rlike(r"^\d+(\.\d+)?$")
df_clean = (df.withColumn("funding_total_usd",
                F.when(is_valid, cleaned.cast(DoubleType())).otherwise(F.lit(None)))
              .dropna(subset=["funding_total_usd"]))
```

### 몬테카를로 포트폴리오 시뮬레이션
```python
for _ in range(num_portfolios):           # 10만 회
    weights = np.random.random(len(categories)); weights /= weights.sum()
    expected_success = np.sum(weights * success_rates) * 100
    funded = np.sum((total_budget_m * weights) / avg_costs_m)
    risk = (100 - expected_success) + max(0, (20 - funded) * 2)
```

---

## 5. 결론 및 제언 (Conclusion)

- **결론:** Spark 기반 집계로 국가·산업별 위험 지형을 파악하고, 이를 몬테카를로 시뮬레이션에 연결해 위험 대비 기대 성공률이 높은 자본 배분안을 정량적으로 제시했습니다.
- **제언:** 투자자는 단일 고성공률 산업에 집중하기보다, 시뮬레이션이 보여준 효율 경계 부근의 분산 배분을 통해 투자 기업 수를 일정 수준 이상 확보하는 전략이 유리합니다.
- **한계:**
  - `category_list`가 파이프(`|`)로 묶인 **멀티태그**라, 일부 조합(예: Biotech & Health 89%)은 소수 표본에서 성공률이 과대평가될 수 있습니다. 향후 주 카테고리(첫 태그) 정규화와 최소 표본 임계치 보정이 필요합니다.
  - 밸류에이션·지분 희석 데이터 부재, 단일 시점 스냅샷, 엑시트 정의의 단순화(acquired/ipo).
  - 리스크 점수는 분산(공분산) 기반이 아닌 휴리스틱 정의로, 향후 Markowitz식 위험 모델로 확장 여지가 있습니다.

---

## 6. 참고 문헌 & 진행 현황

**데이터 출처**
- Kaggle: [Big Startup Success/Fail Dataset from Crunchbase](https://www.kaggle.com/datasets/yanmaksi/big-startup-secsees-fail-dataset-from-crunchbase)

**진행 현황**
- [x] 데이터 수집 및 Spark 전처리
- [x] 국가·산업별 폐업률 분석
- [x] 투자금액–생존율 상관분석
- [x] 몬테카를로 포트폴리오 시뮬레이션 (10만 회)
- [x] 시각화 (효율 경계 · 자본 배분)
- [ ] 멀티태그 카테고리 정규화 보정
- [ ] 발표 자료 준비
# bigdata
