.PHONY: up down logs generate test

# Docker 환경 시작
up:
	cd docker && docker-compose up -d
	@echo "Airflow UI → http://localhost:8080  (admin / admin)"
	@echo "Spark UI  → http://localhost:8081"

# Docker 환경 종료
down:
	cd docker && docker-compose down

# 로그 확인
logs:
	cd docker && docker-compose logs -f airflow-scheduler

# 오늘 날짜 로그 로컬 생성
generate:
	python scripts/log_generator.py --date $(shell date +%Y-%m-%d) --events 100000

# 단위 테스트
test:
	pytest tests/ -v

# DB 스키마 초기화 (postgres 컨테이너 실행 중일 때)
init-db:
	docker exec -i $$(cd docker && docker-compose ps -q postgres) \
	  psql -U airflow -d pipeline_db < sql/init_schema.sql
	@echo "스키마 초기화 완료"
