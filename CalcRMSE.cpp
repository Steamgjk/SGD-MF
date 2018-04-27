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

#define TEST_NAME "./testa.txt"
#define N 71567
#define M 65133
#define K  40 //主题个数
int ITER_NUM  = 100;
int PORTION_NUM = 4;
double P[N][K];
double Q[K][M];
//#define TEST_NAME "./test_out.txt"
using namespace std;
map<long, double> TestMap;

void LoadTestRating()
{
    ifstream ifs(TEST_NAME);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", TEST_NAME);
        exit(-1);
    }
    int cnt = 0;
    int temp = 0;
    long hash_idx = 0;
    double ra = 0;
    while (!ifs.eof())
    {
        ifs >> hash_idx >> ra;
        TestMap.insert(pair<long, double>(hash_idx, ra));
        cnt++;
        if (cnt % 10000 == 0)
        {
            printf("cnt = %ld\n", cnt );
        }
    }
}
double CalcRMSE()
{
    printf("calc RMSE...\n");
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    double sss = 0;
    double avg = 0;
    for (iter = TestMap.begin(); iter != TestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M;
        long col_idx = real_hash_idx % M;
        double sum = 0;
        sss += iter->second;
        for (int k = 0; k < K; k++)
        {
            sum += P[row_idx][k] * Q[k][col_idx];
            //printf("%lf  %lf\n", P[row_idx][k], Q[k][col_idx]);
        }
        if (sum > iter->second)
        {
            positve_cnt++;
        }
        else
        {
            negative_cnt++;

        }
        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }

    rmse /= cnt;
    sss /= cnt;
    rmse = sqrt(rmse);
    printf("positve_cnt=%d negative_cnt=%d sss=%lf\n", positve_cnt, negative_cnt, sss );
    return rmse;
}
int main(int argc, const char * argv[])
{
    ofstream ofs("./rima.txt", ios::trunc);

    if (argc >= 2)
    {
        ITER_NUM = atoi(argv[1]);
    }
    if (argc >= 3)
    {
        PORTION_NUM = atoi(argv[2]);
    }
    LoadTestRating();
    char filename[100];
    for (int i = 1; i < ITER_NUM; i++)
    {
        int row_idx = 0;
        int col_idx = 0;
        for (int j = 0 ; j < PORTION_NUM; j++)
        {
            sprintf(filename, "./track/Pblock-%d-%d", i, j);
            ifstream ifs(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }

            while (!ifs.eof())
            {
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> P[row_idx][kk];
                }
                row_idx++;
            }
            sprintf(filename, "./track/Qblock-%d-%d", i, j);
            ifstream ifs1(filename, ios::in | ios::out);
            if (!ifs1.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }

            while (!ifs1.eof())
            {
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> Q[kk][col_idx];
                }
                col_idx++;
            }
        }
        double rmse = CalcRMSE();
        ofs << rmse << endl;
    }


}


// 1.7