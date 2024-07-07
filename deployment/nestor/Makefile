run: clearLog
	docker compose -f docker-compose.yaml up -d --force-recreate --build nestor

run: stop
	docker compose -f docker-compose.yaml up down nestor

runAll: clearLog
	docker compose -f docker-compose.yaml up -d --force-recreate --build

restartAll: stopAll
	docker compose -f docker-compose.yaml up -d

stopAll:
	docker compose -f docker-compose.yaml stop


clearLog:
	rm /var/log/nestor/nestor.log
	touch /var/log/nestor/nestor.log
	rm /var/log/nestor/.nestor.log.swp || true

clearDocker:
	docker system prune -a