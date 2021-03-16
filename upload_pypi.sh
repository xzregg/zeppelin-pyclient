#!/usr/bin/env bash



function del_build_cacne() {
    rm -rf zeppelin-pyclient.egg-info/
    rm -rf zeppelin-pyclient-*/
    rm -rf build/
    rm -rf dist/
}



del_build_cacne

read -p "build and upload y/n?" ask
if [ $ask == "y" ] ; then
    python setup.py publish
else
    exit
fi


read -p "delete build cache y/n?" ask
if [ $ask == "y" ] ; then
    del_build_cacne
fi