#!/bin/zsh
export DOCDIR='/Users/yangliu/Desktop/Data Engineering/python-env/lambda-project/docs'
export LOGDIR='/Users/yangliu/Desktop/Data Engineering/python-env/lambda-project/logs'
export PYTHON_SCRIPT='/Users/yangliu/Desktop/Data Engineering/python-env/lambda-project/script/pull_data.py'


filenametime1=$(date +"%m%d%Y%H%M%S")

exec > >(tee ${LOGDIR}/script_${filenametime1}.log)
exec 2> >(tee ${LOGDIR}/script_${filenametime1}.log >&2)


echo "STARTING PYTHON SCRIPT TO PULL DATA... ON $(hostname)..."

python $PYTHON_SCRIPT

RC1=$?
if [ $RC1 -ne 0 ]; then
    echo "Python script failed. Exiting..."
    echo "ERROR: ${RC1}"
    echo "ERROR: REFER TO LOG FILE: ${LOGDIR}/script_${filenametime1}.log"
    exit 1
fi

echo "SUCCESSFULLY PROCESSED DATA!"

echo "END RUNNING PROCESS"

echo "PROCESS END AT $(date +"%m%d%Y%H%M%S")"