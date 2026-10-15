@echo off
REM Builds the bridge jar with Maven inside Docker (no local JDK/Maven needed).
REM Output: dist\pas-scada-bridge.jar  -> upload to the server, see README "Release".
setlocal
set HERE=%~dp0
set ROOT=%HERE%..
if not exist "%ROOT%\dist" mkdir "%ROOT%\dist"
if not exist "%ROOT%\dist\.m2" mkdir "%ROOT%\dist\.m2"

docker run --rm ^
  -v "%HERE%:/w" ^
  -v "%ROOT%\dist\.m2:/root/.m2" ^
  -w /w ^
  maven:3.9-eclipse-temurin-17 ^
  mvn -B -q clean package -DskipTests
if errorlevel 1 (
  echo BUILD FAILED
  exit /b 1
)

copy /Y "%HERE%target\PAS-SCADA-Kafka-Bridge-*.jar" "%ROOT%\dist\pas-scada-bridge.jar" >nul
echo.
echo Built: %ROOT%\dist\pas-scada-bridge.jar
dir "%ROOT%\dist\pas-scada-bridge.jar" | find "pas-scada-bridge.jar"
endlocal
