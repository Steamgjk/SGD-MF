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
#define N  17770 // row number
#define M  2649429 //col number
#define K  40 //主题个数
double P[N][K];
double Q[K][M];
#define TEST_NAME "./test_out.txt"
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
        }
        if (sum > iter->second)
        {
            positve_cnt++;
        }
        else
        {
            negative_cnt++;
            //printf("sum = %lf  real=%lf\n", sum, iter->second );
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
    int NUM = 100;
    if (argc >= 2)
    {
        NUM = atoi(argv[1]);
    }
    LoadTestRating();
    srand(time(0));
    double psum = 0;
    for (int i = 0; i < N; i++)
    {
        for (int j = 0; j < K; j++)
        {
            // printf("%lf ", ((double)rand() / RAND_MAX / 10.0) );
            //printf("%lf ", drand48() );
            P[i][j] = drand48() / NUM ;
            psum += P[i][j];

        }
        //printf("\n");
    }
    for (int i = 0; i < M; i++)
    {
        for (int j = 0; j < K; j++)
        {
            // printf("%lf ", ((double)rand() / RAND_MAX / 10.0) );
            //printf("%lf ", drand48() );
            Q[j][i] = drand48() / NUM ;
        }
        //printf("\n");
    }
    double rmse =  CalcRMSE();
    printf("rmse=%lf\n",  rmse);
    printf("randavg=%lf\n", (psum / N ) / K );

}
