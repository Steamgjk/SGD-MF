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
#include <queue>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <queue>
#include <random>
using namespace std;
#define FILE_NAME "./data/TrainingMap"
#define OUT_NAME "./ndata/TrainingMap"
#define M 1000000
#define BK 125000
long hash_ids[8][8][500000];
double rates[8][8][500000];
long heads[8][8];
long tails[8][8];
void write_data(int td_id);
void write_data(int td_id)
{
	char fn[100];
	sprintf(fn, "%s-%d", OUT_NAME, td_id);
	ofstream ofs(fn, ios::trunc);
	int row = td_id / 8;
	int col = td_id % 8;
	while (1 == 1)
	{
		if (heads[row][col] < tails[row][col])
		{
			long h = heads[row][col];
			ofs << hash_ids[row][col][h] << " " << rates[row][col][h] << endl;
			heads[row][col]++;
			if (heads[row][col] % 10000 == 0)
			{

				printf("W[%d][%d] %ld\n", row, col, heads[row][col] );
				//getchar();
			}
		}
	}
}
int main()
{
	for (int i = 0; i < 8; i++)
	{
		for (int j = 0; j < 8; j++)
		{
			heads[i][j] = 0;
			tails[i][j] = 0;
		}
	}
	for (int i = 0 ; i < 64; i++)
	{
		std::thread write_td(write_data, i);
		write_td.detach();
	}
	char fn[100];
	long hash_id;
	double rate;
	long row = 0;
	long col = 0;
	long brow = 0;
	long bcol = 0;
	long h, t;
	h = t = 0;
	for (int i = 0; i < 63; i++)
	{
		sprintf(fn, "%s-%d", FILE_NAME, i);
		ifstream ifs(fn);
		while (!ifs.eof())
		{
			ifs >> hash_id >> rate;
			row = hash_id / M;
			col = hash_id % M;
			brow = row / BK;
			bcol = col / BK;
			t = tails[brow][bcol];
			hash_ids[brow][bcol][t] = hash_id;
			rates[brow][bcol][t] = rate;
			tails[brow][bcol]++;
			if (tails[brow][bcol] % 10000 == 0)
			{
				//printf("brow = %ld bcol %ld\n", brow, bcol );
				printf("[%ld][%ld] %ld\n", brow, bcol, tails[brow][bcol] );
				//getchar();
			}


		}
	}
	while (1 == 1)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

}

//g++  -g  -std=c++11  -pthread newprocess.cpp -o newprocess