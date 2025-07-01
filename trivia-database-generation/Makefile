serve:
	sudo docker-compose --env-file .env.development up -d
setup:
	python3 -m venv venv && ./venv/bin/pip3 install -r requirements.txt
run:
	./venv/bin/python3 load_database.py
