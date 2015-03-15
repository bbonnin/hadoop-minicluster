@echo off
rem Replace windows file separator by unix file separator
rem for /f %%i in ('call bash "echo %* | sed 's/\\/r/g'"') do set params=%%i
for /f %%i in ('call bash -c ls') do set params=%%i
echo %params%
call bash -c "%*"
