#!/bin/bash

cd /home/lzx/bustub/build

make lru_replacer_test -j8 
make buffer_pool_manager_instance_test -j8

./test/lru_replacer_test
./test/buffer_pool_manager_instance_test

cd ..
