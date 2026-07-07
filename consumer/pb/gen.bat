@echo off
cd /d "%~dp0"

protoc --go_out=.. --go_opt=module=dgdemo --go-triple_out=.. --go-triple_opt=module=dgdemo ./*.proto
