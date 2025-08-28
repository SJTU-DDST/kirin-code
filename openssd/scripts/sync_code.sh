#!/bin/bash

VITIS_WORKSPACE_DIR=/home/guifeng/compaction_with_search_engine_vitiside
VITIS_FTL_PROJECT_NAME=ftl
VITIS_COMPACTION_PROJECT_NAME=compaction
VITIS_EMU_PROJECT_NAME=emu
VITIS_SEARCH_PROJECT_NAME=search

pushd $(dirname $0) > /dev/null

sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_FTL_PROJECT_NAME}/src/*.c
sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_FTL_PROJECT_NAME}/src/*.h
find ${VITIS_WORKSPACE_DIR}/${VITIS_FTL_PROJECT_NAME}/src -mindepth 1 -maxdepth 1 -type d | xargs sudo rm -rf

sudo cp -r ../ftl/* ${VITIS_WORKSPACE_DIR}/${VITIS_FTL_PROJECT_NAME}/src
sudo cp -r ../common/* ${VITIS_WORKSPACE_DIR}/${VITIS_FTL_PROJECT_NAME}/src

sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_COMPACTION_PROJECT_NAME}/src/*.c
sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_COMPACTION_PROJECT_NAME}/src/*.h
find ${VITIS_WORKSPACE_DIR}/${VITIS_COMPACTION_PROJECT_NAME}/src -mindepth 1 -maxdepth 1 -type d | xargs sudo rm -rf

sudo cp -r ../compaction/* ${VITIS_WORKSPACE_DIR}/${VITIS_COMPACTION_PROJECT_NAME}/src
sudo cp -r ../common/* ${VITIS_WORKSPACE_DIR}/${VITIS_COMPACTION_PROJECT_NAME}/src

sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_EMU_PROJECT_NAME}/src/*.c
sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_EMU_PROJECT_NAME}/src/*.h
find ${VITIS_WORKSPACE_DIR}/${VITIS_EMU_PROJECT_NAME}/src -mindepth 1 -maxdepth 1 -type d | xargs sudo rm -rf

sudo cp -r ../emu/* ${VITIS_WORKSPACE_DIR}/${VITIS_EMU_PROJECT_NAME}/src
sudo cp -r ../common/* ${VITIS_WORKSPACE_DIR}/${VITIS_EMU_PROJECT_NAME}/src

sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_SEARCH_PROJECT_NAME}/src/*.c
sudo rm -f ${VITIS_WORKSPACE_DIR}/${VITIS_SEARCH_PROJECT_NAME}/src/*.h
find ${VITIS_WORKSPACE_DIR}/${VITIS_SEARCH_PROJECT_NAME}/src -mindepth 1 -maxdepth 1 -type d | xargs sudo rm -rf

sudo cp -r ../search/* ${VITIS_WORKSPACE_DIR}/${VITIS_SEARCH_PROJECT_NAME}/src
sudo cp -r ../common/* ${VITIS_WORKSPACE_DIR}/${VITIS_SEARCH_PROJECT_NAME}/src

popd > /dev/null
