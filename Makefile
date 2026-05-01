
export COMPOSE_PROJECT_NAME = kyuff

test:
	go test ./... -count 1 -race

test-coverage:
	go test -coverprofile=coverage.txt ./... -count 1 -race

vet:
	go vet ./...

cover:
	go test ./... -count 1 -race -cover

gen:
	go generate ./...

plantuml-docker:
	docker run -v $(shell pwd)/docs:/docs -w /docs ghcr.io/plantuml/plantuml *.pu

plantuml:
	plantuml docs/*.pu

up:
	docker compose up postgres -d --wait
	docker compose exec postgres psql -U postgres -c "CREATE DATABASE es" 2>/dev/null || true

down:
	docker compose down --remove-orphans --volumes

restart: down up
