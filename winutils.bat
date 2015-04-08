@echo off

echo WINUTILS : %* >> winutils.log

set tempFile=winutils.%RANDOM%


if ""%1"" == ""symlink"" (
  goto symlink  
) else if ""%1"" == ""perm"" (
  goto perm
) else (
  call bash -c "%*"
  goto fin
)


:symlink
cygpath -u "%2" > %tempFile%  
set /p target= < %tempFile%
cygpath -u "%3" > %tempFile%  
set /p src= < %tempFile%
call bash -c "ln -s %target% %src%"
goto fin

:perm
cygpath -u "%2" > %tempFile%
set /p target= < %tempFile%
echo WINUTILS PERM : %target% >> winutils.log
call bash -c "ls -ld %target%"
goto fin


:fin
del %tempFile%