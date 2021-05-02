.DEFAULT_GOAL := all

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt
		
	sudo apt install ffmpeg

format:
	black *.py
	
all: install run