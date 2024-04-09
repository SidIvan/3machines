run1: clearLog
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	rm /var/log/writer/.writer.log.swp || true
	docker compose -f docker-compose1.yaml build --no-cache
	docker compose -f docker-compose1.yaml up -d --force-recreate

restart1: stop1
	docker compose -f docker-compose1.yaml up -d

stop1:
	docker compose -f docker-compose1.yaml stop

run2: clearLog
	docker compose -f docker-compose2.yaml build --no-cache
	docker compose -f docker-compose2.yaml up -d --force-recreate

restart2: stop2
	docker compose -f docker-compose2.yaml up -d

stop2:
	docker compose -f docker-compose2.yaml stop


clearLog:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	rm /var/log/writer/.writer.log.swp || true

clearDocker:
	docker system prune -a