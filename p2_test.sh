cd /home/lzx/bustub/build

make hash_table_page_test -j8 
make hash_table_test -j8

./test/hash_table_page_test
./test/hash_table_test

cd ..