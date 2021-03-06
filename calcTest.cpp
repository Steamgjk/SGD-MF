#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <sstream>
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
	int lcnt = 0;
	string line;
	while (!ifs2.eof())
	{
		getline(ifs2, line);
		stringstream ss(line);
		if (line[line.length() - 1] == ':')
		{
			ss >> mov_id;
			//mov_id--;
		}
		else
		{
			//cout << line << endl;
			ss >> cus_id;
			//cus_id--;

			hash_id = (mov_id - 1) * M + (cus_id - 1);
			//printf("mov_id=%ld cus_id=%ld hash_id=%ld\n", mov_id, cus_id, hash_id );
			//getchar();
			iter = mm.find(hash_id);
			if (iter != mm.end())
			{
				ofs << iter->first << " " << iter->second << endl;
				//printf("oof %ld  %lf\n", iter->first, iter->second);
				//getchar();
			}
			else
			{
				//mov_id=10000 cus_id=200206 hash_id=26491840776
				printf("mov_id=%ld cus_id=%ld hash_id=%ld\n", mov_id, cus_id, hash_id );
				getchar();
				ofs << iter->first << " " << 0 << endl;
			}
			if (lcnt % 1000 == 0)
			{
				cout << lcnt << endl;
			}
		}
		lcnt++;

	}
}