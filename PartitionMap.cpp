#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <map>
using namespace std;
#define FILE_NAME "./netflix_row.txt"
#define N  17770 // row number
#define M  2649429 //col number
#define K  40 //主题个数

int P = 16;
std::map<long, double> Rmap;
std::map<long, double> SMap[20][20];
#define SEQ_FILE "./seq.txt"
int main(int argc, const char * argv[])
{
	if (argc >= 2)
	{
		P = atoi(argv[1]);
	}
	std::vector<long> row_vec(P);
	std::vector<long> col_vec(P);
	std::vector<long> row_len(P);
	std::vector<long> col_len(P);
	long row_unit_len = N / P;
	long col_unit_len = M / P;
	for (int i = 0; i < P; i++)
	{
		row_len[i] = row_unit_len;
		col_len[i] = col_unit_len;
	}
	long res_len = N - N / P * P;
	for (int i = 0; i < res_len; i++)
	{
		row_len[i]++;
	}
	res_len = M - M / P * P;
	for (int i = 0; i < res_len; i++)
	{
		col_len[i]++;
	}
	row_vec[0] = col_vec[0] = 0;
	for (int i = 1; i < P; i++)
	{
		row_vec[i] = row_vec[i - 1] + row_len[i];
		col_vec[i] = col_vec[i - 1] + col_len[i];
	}
	ifstream ifs(FILE_NAME);
	if (!ifs.is_open())
	{
		printf("open fail %s\n", FILE_NAME );
		exit(-1);
	}
	long hash_id; double rate;
	int cnt = 0;
	while (!ifs.eof())
	{
		ifs >> hash_id >> rate;
		cnt++;
		if (cnt % 1000000 == 0 )
		{
			printf("cnt = %d\n", cnt );
		}
		long row_id = hash_id / M;
		long col_id = hash_id % M;
		bool has  = false;
		for (int i = 0; i < P; i++)
		{
			if (row_vec[i] <= row_id && row_id < row_vec[i] + row_len[i])
			{
				for (int j = 0 ; j < P; j++)
				{
					if (col_vec[i] <= col_id && col_id < col_vec[i] + col_len[i])
					{
						has = true;
						SMap[i][j].insert(pair<long, double>(hash_id, rate));
						break;
					}
				}
				if (has)
				{
					break;
				}
			}

		}
	}
	printf("Writing...\n");
	char filne[100];
	for (int i = 0 ; i < P; i++)
	{
		for (int j = 0; j < P; j++)
		{
			int block_id = i * P + j;
			sprintf(filne, "./tmp/%s-%d", FILE_NAME, block_id);
			printf("%s\n", filne);
			ofstream ofs(filne, ios::trunc);
			map<long, double>::iterator iter;
			for (iter = SMap[i][j].begin(); iter != SMap[i][j].end(); iter++)
			{
				ofs << iter->first << " " << iter->second << endl;
			}
		}
	}



}