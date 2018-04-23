#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <string>
#include <map>
using namespace std;

#define FILE_NAME "./netflix_mtx.txt"
#define N  17770 // row number
#define M  2649429 //col number

#define ROW_NAME "./netflix_row.txt"
#define TEST_NAME "./probe.txt"
#define TEST_OUT "./test_out.txt"
map<long, double> mm;
int main()
{

	ifstream ifs(ROW_NAME);
	if (!ifs.is_open())
	{
		printf("fail to open the file %s\n", FILE_NAME);
		exit(-1);
	}
	long real_hash_idx;
	double rate;
	int line_cnt = 0;
	while (!ifs.eof())
	{
		ifs >> real_hash_idx >> rate;
		mm.insert(pair<long, double>(real_hash_idx, rate));
		line_cnt++;
		if (line_cnt % 1000000 == 0)
		{
			printf("line_cnt = %d\n", line_cnt );
		}
	}
	ifstream ifs2(TEST_NAME);
	if (!ifs2.is_open())
	{
		printf("fail to open the file %s\n", TEST_NAME);
		exit(-1);
	}
	long mov_id = 0;
	long cus_id = 0;
	char tp;
	string tmst;
	long hash_id = 0;
	map<long, double>::iterator iter;
	ofstream ofs(TEST_OUT, ios::trunc);
	printf("Haha  sz = %ld\n", mm.size());
	while (!ifs2.eof())
	{
		ifs2 >> cus_id >> tp;
		if (tp == ':')
		{
			mov_id = cus_id;
		}
		else if (tp == ',')
		{
			ifs2 >> tmst;
			hash_id = mov_id * M + cus_id;
			iter = mm.find(hash_id);
			if (iter != mm.end())
			{
				ofs << iter->first << " " << iter->second << endl;
				printf("oof %ld  %ld\n", iter->first, iter->second);
			}
			else
			{
				ofs << iter->first << " " << 0 << endl;
			}
		}
		else
		{
			cout << cus_id << "\t" << tp << endl;
		}
	}
}