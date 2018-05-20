//
//  main.cpp
//  linux_socket_api
//
//  Created by bikang on 16/11/2.
//  Copyright (c) 2016年 bikang. All rights reserved.
//

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

#include "client_rdma_op.h"
#include "server_rdma_op.h"
using namespace std;
#define GROUP_NUM 1
#define DIM_NUM 4

#define BLOCK_MEM_SZ (250000000)
#define MEM_SIZE (BLOCK_MEM_SZ*2)
char* to_send_block_mem;
char* to_recv_block_mem;

//cnt=15454227 sizeof(long)=8
//#define FILE_NAME "./netflix_row.txt"
//#define TEST_NAME "./test_out.txt"
//#define N  17770 // row number
//#define M  2649429 //col number
//#define K  40 //主题个数

//#define FILE_NAME "./movielen10M_train.txt"
//#define TEST_NAME "./movielen10M_test.txt"


#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数


/*
#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数
**/

#define CAP 200
#define WORKER_NUM 1
#define WORKER_N_1 4
#define QP_GROUP 1

char* remote_ips[CAP] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int remote_ports[CAP] = {4411, 4412, 4413, 4414};

char* local_ips[CAP] = {"12.12.10.12", "12.12.10.15", "12.12.10.19", "12.12.10.17"};
int local_ports[CAP] = {5511, 5512, 5513, 5514};

int send_round_robin_idx = 0;
int recv_round_robin_idx = 0;
#define ThreshIter 1000
#define SEQ_LEN 5000
#define WORKER_THREAD_NUM 30

struct Block
{
    int block_id;
    int data_age;
    int sta_idx;
    int height; //height
    int ele_num;
    bool isP;
    vector<double> eles;
    Block()
    {

    }
    Block operator=(Block& bitem)
    {
        block_id = bitem.block_id;
        data_age = bitem.data_age;
        height = bitem.height;
        eles = bitem.eles;
        ele_num = bitem.ele_num;
        sta_idx = bitem.sta_idx;
        return *this;
    }
    void printBlock()
    {

        printf("block_id  %d\n", block_id);
        printf("data_age  %d\n", data_age);
        printf("ele_num  %d\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");

    }
};
struct Updates
{
    int block_id;
    int clock_t;
    int ele_num;
    vector<double> eles;
    Updates()
    {

    }
    Updates operator=(Updates& uitem)
    {
        block_id = uitem.block_id;
        clock_t = uitem.clock_t;
        ele_num = uitem.ele_num;
        eles = uitem.eles;
        return *this;
    }

    void printUpdates()
    {
        printf("update block_id %d\n", block_id );
        printf("clock_t  %d\n", clock_t);
        printf("ele size %ld\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");
    }
};
struct Block Pblock;
struct Block Qblock;
struct Updates Pupdt;
struct Updates Qupdt;
vector<double> oldP;
vector<double> oldQ;
vector<long> hash_ids;
std::vector<long> rates;

bool canSend = false;
bool hasRecved = false;
int block_seq[SEQ_LEN];


double yita = 0.003;
double theta = 0.01;

/* Jumbo **/
/*
double yita = 0.002;
double theta = 0.05;
**/

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);

void rdma_sendTd(int send_thread_id);
void rdma_recvTd(int recv_thread_id);

//void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);
//void submf(Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.1, float beta = 0.1);
void submf();
void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
void LoadRmatrix(int file_no, map<long, double>& myMap);
void CalcUpdt(int thread_id);
void LoadData();
void LoadRequiredData(int row, int col, int data_idx);
void InitFlag();

int thread_id = -1;
struct timeval start, stop, diff;
vector<bool> StartCalcUpdt;
map<long, double> RMap;
map<long, double> RMaps[8][8];



std::vector<long> hash_for_row_threads[10][10][WORKER_THREAD_NUM];
std::vector<double> rates_for_row_threads[10][10][WORKER_THREAD_NUM];
std::vector<long> hash_for_col_threads[10][10][WORKER_THREAD_NUM];
std::vector<double> rates_for_col_threads[10][10][WORKER_THREAD_NUM];

int main(int argc, const char * argv[])
{

    for (int i = 0; i < CAP; i++)
    {
        local_ports[i] = 55511 + i;
        remote_ports[i] = 44411 + i;
    }
    int thresh_log = 2000;
    thread_id = atoi(argv[1]);
    to_send_block_mem = (void*)malloc(MEM_SIZE);
    to_recv_block_mem = (void*)malloc(MEM_SIZE);
    InitFlag();
    printf("to_send_block_mem=%p  to_recv_block_mem=%p\n", to_send_block_mem, to_recv_block_mem );
    if (argc >= 3)
    {
        thresh_log = atoi(argv[2]);
    }

    for (int i = 0; i < QP_GROUP; i++)
    {
        int th_id = thread_id + i * WORKER_N_1;
        printf("recv th_id=%d\n", th_id );
        std::thread recv_thread(rdma_recvTd, th_id);
        recv_thread.detach();
    }


    printf("wait for you for 3s\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));


    for (int i = 0; i < QP_GROUP; i++)
    {
        int th_id = thread_id + i * WORKER_N_1;
        std::thread send_thread(rdma_sendTd, th_id);
        send_thread.detach();
    }



    StartCalcUpdt.resize(WORKER_THREAD_NUM);
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        StartCalcUpdt[i] = false;
    }
    canSend = false;
    memset(&start, 0, sizeof(struct timeval));
    memset(&stop, 0, sizeof(struct timeval));
    memset(&diff, 0, sizeof(struct timeval));

    /*
        std::thread send_thread(sendTd, thread_id);
        send_thread.detach();

        std::thread recv_thread(recvTd, thread_id);
        recv_thread.detach();
    **/

    int iter_cnt = 0;
    bool isstart = false;

    LoadData();
    printf("Load Rating Success\n");

    std::vector<thread> td_vec;
    //printf("wait for you for 3s\n");
    //std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        //std::thread td(CalcUpdt, i);
        td_vec.push_back(std::thread(CalcUpdt, i));
    }
    //printf("come here\n");
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec[i].detach();
        //printf("%d  has detached\n", i );
    }
    printf("detached well\n");

    while (1 == 1)
    {
        //printf(" hasRecved? %d\n", hasRecved);
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        if (hasRecved)
        {
            //printf("has Received\n");
            if (!isstart)
            {
                isstart = true;
                gettimeofday(&start, 0);
            }

            //SGD
            int row_sta_idx = Pblock.sta_idx;
            int row_len = Pblock.height;
            int col_sta_idx = Qblock.sta_idx;
            int col_len = Qblock.height;
            int ele_num = row_len * col_len;
            //printf("before submf\n");
            submf();
            //printf("after submf\n");
            iter_cnt++;

            /*
            if (iter_cnt % 10 == 0)
            {
                WriteLog(Pblock, Qblock, iter_cnt);
            }
            **/

            if (iter_cnt == thresh_log )
            {
                gettimeofday(&stop, 0);

                long long mksp = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
                printf("itercnt = %d  time = %lld\n", iter_cnt, mksp);
                //WriteLog(Pblock, Qblock, iter_cnt);
                //exit(0);
            }
            canSend = true;
            //printf("canSend = true\n");
            hasRecved = false;

        }
    }


}
void InitFlag()
{
    int* pb = (int*)(void*)(to_recv_block_mem);
    *pb = -1;
    pb = (int*)(void*)(to_recv_block_mem + BLOCK_MEM_SZ);
    *pb = -1;
}
void LoadRmatrix(int file_no, map<long, double>& myMap)
{
    char fn[100];
    sprintf(fn, "%s%d", FILE_NAME, file_no);
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }
    int cnt = 0;
    long hash_idx = -1;
    double ra = 0;
    long row_idx, col_idx;

    while (!ifs.eof())
    {
        ifs >> hash_idx >> ra;
        if (hash_idx >= 0)
        {
            myMap.insert(pair<long, double>(hash_idx, ra));
            cnt++;
            if (cnt % 1000000 == 0)
            {
                printf("cnt = %ld\n", cnt );
            }
        }
    }
}

void LoadData()
{
    char fn[100];
    long hash_id;
    double rate;
    long cnt = 0;
    for (int row = 0; row < WORKER_NUM; row++)
    {
        for (int col = 0; col < WORKER_NUM; col++)
        {
            for (int td = 0; td < WORKER_THREAD_NUM; td++)
            {
                hash_for_row_threads[row][col][td].clear();
                rates_for_row_threads[row][col][td].clear();
                hash_for_col_threads[row][col][td].clear();
                rates_for_col_threads[row][col][td].clear();
            }

        }
    }
    for (int data_idx = 0; data_idx < 64; data_idx++)
    {
        int row = data_idx / DIM_NUM;
        int col = data_idx % DIM_NUM;
        row /= 2;
        col /= 2;
        sprintf(fn, "%s%d", FILE_NAME, data_idx);
        //printf("fn=%s  :[%d][%d]\n", fn, row, col );
        ifstream ifs(fn);
        if (!ifs.is_open())
        {
            printf("fail to open %s\n", fn );
            exit(-1);
        }
        cnt = 0;
        long ridx, cidx;

        while (!ifs.eof())
        {
            hash_id = -1;
            ifs >> hash_id >> rate;
            if (hash_id >= 0)
            {
                ridx = ((hash_id) / M) % WORKER_THREAD_NUM;
                cidx = ((hash_id) % M) % WORKER_THREAD_NUM;

                hash_for_row_threads[row][col][ridx].push_back(hash_id);
                rates_for_row_threads[row][col][ridx].push_back(rate);
                hash_for_col_threads[row][col][cidx].push_back(hash_id);
                rates_for_col_threads[row][col][cidx].push_back(rate);
            }
        }
    }
}

void LoadRequiredData(int row, int col, int data_idx)
{
    char fn[100];
    long hash_id;
    double rate;
    long cnt = 0;



    sprintf(fn, "%s%d", FILE_NAME, data_idx);
    printf("fn=%s  :[%d][%d]\n", fn, row, col );
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open %s\n", fn );
        exit(-1);
    }
    cnt = 0;
    long ridx, cidx;
    hash_id = -1;
    while (!ifs.eof())
    {
        ifs >> hash_id >> rate;
        if (hash_id >= 0)
        {
            ridx = ((hash_id) / M) % WORKER_THREAD_NUM;
            cidx = ((hash_id) % M) % WORKER_THREAD_NUM;
            hash_for_row_threads[row][col][ridx].push_back(hash_id);
            rates_for_row_threads[row][col][ridx].push_back(rate);
            hash_for_col_threads[row][col][cidx].push_back(hash_id);
            rates_for_col_threads[row][col][cidx].push_back(rate);
        }

    }

}


void WriteLog(Block&Pb, Block&Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./PS-track/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "./PS-track/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
}


void CalcUpdt(int td_id)
{

    while (1 == 1)
    {

        int p_block_idx = Pblock.block_id;
        int q_block_idx = Qblock.block_id;
        if (StartCalcUpdt[td_id] == true)
        {
            //printf("enter CalcUpdt\n");
            int times_thresh = 200;
            int row_sta_idx = Pblock.sta_idx;
            int col_sta_idx = Qblock.sta_idx;
            size_t rtsz;
            size_t ctsz;
            rtsz = hash_for_row_threads[p_block_idx][q_block_idx][td_id].size();
            ctsz = hash_for_col_threads[p_block_idx][q_block_idx][td_id].size();
            if (rtsz == 0 || ctsz == 0)
            {
                //printf("p %d q %d td=%d empty\n", p_block_idx, q_block_idx, td_id );
                //exit(0);
                StartCalcUpdt[td_id] = false;
                continue;
            }
            int rand_idx = -1;
            for (times_thresh; times_thresh > 0; times_thresh--)
            {
                //printf("times_thresh=%d\n", times_thresh );
                rand_idx = random() % rtsz;
                long real_hash_idx = hash_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                long i = real_hash_idx / M - row_sta_idx;
                long j = real_hash_idx % M - col_sta_idx;
                double error = rates_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                if (i < 0 || j < 0 || i >= Pblock.height || j >= Qblock.height)
                {
                    //printf("[%d] continue i=%ld j=%ld  ph=%d  qh=%d \n", td_id, i, j , Pblock.height, Qblock.height);
                    //getchar();
                    continue;
                }
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Pblock.eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                    if (Pblock.eles[i * K + k] + 1 == Pblock.eles[i * K + k] - 1)
                    {
                        printf("p %d q %d  error =%lf i=%d j=%d k=%d rand_idx=%d vale=%lf pvale=%lf  qvalue=%lf\n", p_block_idx, q_block_idx, error, i, j, k, rand_idx,  rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx], oldP[i * K + k], oldQ[j * K + k] );
                        getchar();
                    }
                }

                rand_idx = random() % ctsz;
                real_hash_idx = hash_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                if (i < 0 || j < 0 || i >= Pblock.height || j >= Qblock.height)
                {
                    //printf("[%d] continue l11 \n", td_id);
                    continue;
                }
                error = rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];

                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Qblock.eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
                    if (Qblock.eles[j * K + k] + 1 == Qblock.eles[j * K + k] - 1)
                    {
                        printf("p %d q %d  error =%lf i=%d j=%d k=%d rand_idx=%d vale=%lf pvale=%lf  qvalue=%lf\n", p_block_idx, q_block_idx, error, i, j, k, rand_idx,  rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx], oldP[i * K + k], oldQ[j * K + k] );
                        getchar();

                    }
                }
            }
            //printf("Fini %d\n", td_id);
            StartCalcUpdt[td_id] = false;


        }

    }


}
void submf()
{
    double error = 0;
    int minN = Pblock.height;
    int minM = Qblock.height;

    int row_sta_idx = Pblock.sta_idx;
    int col_sta_idx = Qblock.sta_idx;
    int row_len = Pblock.height;
    int col_len = Qblock.height;

    int Psz = Pblock.height * K;
    int Qsz = Qblock.height * K;
    //printf("copying ...\n");
    oldP = Pblock.eles;
    //printf("copy P fin ele=%ld Psz = %d Ph=%d K=%d\n", oldP.size(), Psz, Pblock.height, K);
    oldQ = Qblock.eles;
    //printf("copy Q fin ele=%ld Qsz=%d Qh=%d  K=%d\n", oldQ.size(), Qsz, Qblock.height, K);
    //getchar();
    for (int i = 0; i < Psz; i++)
    {
        //printf("[%d]:%lf\n", i, oldP[i] );
        if (oldP[i] > 100 || oldP[i] < -100)
        {
            printf("P Exception! [%d] %lf\n", i, oldP[i]);
            getchar();
        }

    }
    //printf("comere hhe\n");
    for (int i = 0; i < Qsz; i++)
    {
        if (oldQ[i] > 100 || oldQ[i] < -100)
        {
            printf("Q Exception! [%d] %lf\n", i, oldQ[i]);
            getchar();
        }

    }
    //printf("enter submf22\n");
    struct timeval beg, ed;
    long long mksp;
    gettimeofday(&beg, 0);

    /*
        int r1 = Pblock.block_id * 2;
        int c1 = Qblock.block_id * 2;
        int f1 = r1 * 8 + c1;
        int f2 = r1 * 8 + c1 + 1;
        int f3 = (r1 + 1) * 8 + c1;
        int f4 = (r1 + 1) * 8 + c1 + 1;

        int row = Pblock.block_id;
        int col = Qblock.block_id;
        //printf("row=%d col=%d\n", row, col );
        for (int td = 0; td < WORKER_THREAD_NUM; td++)
        {
            hash_for_row_threads[row][col][td].clear();
            rates_for_row_threads[row][col][td].clear();
            hash_for_col_threads[row][col][td].clear();
            rates_for_col_threads[row][col][td].clear();
        }
        LoadRequiredData(row, col, f1);
        LoadRequiredData(row, col, f2);
        LoadRequiredData(row, col, f3);
        LoadRequiredData(row, col, f4);
        **/
    /*
    for (int td = 0; td < WORKER_THREAD_NUM; td++)
    {
        {
            printf("[%d][%d] %ld\n", row, col, hash_for_row_threads[row][col][td] );
        }
    }
    **/
    gettimeofday(&ed, 0);
    mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
    printf("Load time = %lld\n", mksp);

    bool canbreak = true;
    for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
    {
        StartCalcUpdt[ii] = 1;
    }

    while (1 == 1)
    {
        canbreak = true;
        for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
        {

            if (StartCalcUpdt[ii] == true)
            {
                //printf("ii=%d, %d \n", ii, StartCalcUpdt[ii] );
                canbreak = false;
            }

        }
        if (canbreak)
        {
            break;
        }
    }


    gettimeofday(&ed, 0);
    mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
    printf("Calc  time = %lld\n", mksp);

}





int wait4connection(char*local_ip, int local_port)
{
    int fd = socket(PF_INET, SOCK_STREAM , 0);
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(local_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, local_ip, &address.sin_addr);
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding...\n");
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);
    printf("bind ok\n");
    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);

    printf("thread %d listening at %s %d\n", thread_id, local_ip, local_port );
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}


void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];
    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    while (1 == 1)
    {
        //printf("canSend=%d\n", canSend );
        if (canSend)
        {
            //printf("Td:%d cansend\n", thread_id );
            size_t struct_sz = sizeof(Block);
            size_t data_sz = sizeof(double) * Pblock.ele_num;
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblock), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblock.eles[0]), data_sz);

            size_t total_len = struct_sz + data_sz;
            //printf("total_len=%ld struct_sz=%ld data_sz=%ld  elenum=%d\n", total_len, struct_sz, data_sz, Pblock.ele_num );
            struct timeval st, et, tspan;
            size_t sent_len = 0;
            size_t remain_len = total_len;
            int ret = -1;
            size_t to_send_len = 4096;
            //gettimeofday(&st, 0);


            while (remain_len > 0)
            {
                if (to_send_len > remain_len)
                {
                    to_send_len = remain_len;
                }
                //printf("sending...\n");
                ret = send(fd, buf + sent_len, to_send_len, 0);
                if (ret >= 0)
                {
                    remain_len -= to_send_len;
                    sent_len += to_send_len;
                    //printf("remain_len = %ld\n", remain_len);
                }
                else
                {
                    printf("still fail\n");
                }
                //getchar();
            }
            free(buf);


            data_sz = sizeof(double) * Qblock.ele_num;
            total_len = struct_sz + data_sz;
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblock), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblock.eles[0]), data_sz);
            //printf("Q  total_len=%ld struct_sz=%ld data_sz=%ld ele_num=%d\n", total_len, struct_sz, data_sz, Qblock.ele_num );
            sent_len = 0;
            remain_len = total_len;
            ret = -1;
            to_send_len = 4096;
            while (remain_len > 0)
            {
                if (to_send_len > remain_len)
                {
                    to_send_len = remain_len;
                }
                //printf("sending...\n");
                ret = send(fd, buf + sent_len, to_send_len, 0);
                if (ret >= 0)
                {
                    remain_len -= to_send_len;
                    sent_len += to_send_len;
                }
                else
                {
                    printf("still fail\n");
                }
            }

            free(buf);
            /*
            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("send two blocks mksp=%lld\n", mksp );
            **/
            canSend = false;
        }
        else
        {
            //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

}

void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    while (1 == 1)
    {
        //printf("recv loop\n");
        struct timeval st, et;

        gettimeofday(&st, 0);

        size_t expected_len = sizeof(Pblock);
        char* sockBuf = (char*)malloc(expected_len + 100);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {
            //printf("recving..\n");
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            //printf("check 1.5\n");
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        size_t data_sz = sizeof(double) * (Pblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            //printf("recving 2\n");
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        //printf("check 5\n");
        double* data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Pblock.ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
            if (Pblock.eles[i] > 100 || Pblock.eles[i] < -100 )
            {
                printf("P Exception!\n");
            }
        }
        free(data_eles);

        expected_len = sizeof(Pblock);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* qb = (struct Block*)(void*)sockBuf;
        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        //printf("recv pele %d qele %d\n", Pblock.ele_num, Qblock.ele_num );
        free(sockBuf);

        data_sz = sizeof(double) * (Qblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Qblock.ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
            if (Qblock.eles[i] > 100 || Qblock.eles[i] < -100 )
            {
                printf("Q Exception!\n");
            }
        }
        free(data_eles);

        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        //printf("recv two blocks time = %lld\n", mksp);

        hasRecved = true;
    }
}



void rdma_sendTd(int send_thread_id)
{
    printf("[%d] worker send waiting for 3s...\n", send_thread_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    char* remote_ip = remote_ips[send_thread_id % WORKER_N_1];
    int remote_port = remote_ports[send_thread_id];

    struct sockaddr_in server_sockaddr;
    int ret, option;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    get_addr(remote_ip, (struct sockaddr*) &server_sockaddr);
    server_sockaddr.sin_port = htons(remote_port);

    client_rdma_op cro;
    ret = cro.client_prepare_connection(&server_sockaddr);
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }
    ret = cro.client_pre_post_recv_buffer();
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }
    ret = cro.client_connect_to_server();
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }

    ret = cro.client_send_metadata_to_server1(to_send_block_mem, MEM_SIZE);
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }

    char*buf = NULL;
    while (1 == 1)
    {
        int real_total = 0;
        size_t p_total = 0;
        size_t q_total = 0;
        size_t struct_sz = sizeof( Block);
        size_t p_data_sz = 0;
        size_t q_data_sz = 0;
        int total_len = 0;
        int check_sum = 0;
        struct timeval st, et, tspan;
        int*flag = (int*)(void*) to_send_block_mem;
        *flag = -1;
        char* real_sta_buf = to_send_block_mem + sizeof(int);
        if (canSend)
        {

            buf = to_send_block_mem;

            p_data_sz = sizeof(double) * Pblock.ele_num;
            q_data_sz = sizeof(double) * Qblock.ele_num;
            p_total = struct_sz + p_data_sz;
            q_total = struct_sz + q_data_sz;
            total_len = p_total + q_total;
            real_total = total_len + sizeof(int) + sizeof(int);
            char* real_sta_buf = buf + sizeof(int);

            *flag = total_len;
            memcpy(real_sta_buf, &(Pblock), struct_sz);
            memcpy(real_sta_buf + struct_sz, (char*) & (Pblock.eles[0]), p_data_sz);
            memcpy(real_sta_buf + p_total, &(Qblock), struct_sz);
            memcpy(real_sta_buf + p_total + struct_sz , (char*) & (Qblock.eles[0]), q_data_sz);
            memcpy(real_sta_buf + total_len, &total_len, sizeof(int));

            int* tmp = (int*)(void*)buf;
            printf("head =%d  %d\n", (*buf), (*tmp) );
            ret = cro.start_remote_write(real_total, 0);
            printf("[%d]:writer another block success real_total=%ld\n", send_thread_id, real_total);

            //printf("flag=%d\n", (*flag) );
            //ret = cro.start_remote_write(sizeof(int), 0);
            printf("[%d]:send flag\n", send_thread_id);
            send_round_robin_idx = (send_round_robin_idx + 1) % QP_GROUP;



            //printf("all a\n");
            getchar();
            canSend = false;
        }
    }

}
void rdma_recvTd(int recv_thread_id)
{
    printf("rdma_recv thread_id = %d\n local_ip-11=%s  local_port-11=%d\n", recv_thread_id, local_ips[recv_thread_id % WORKER_N_1], local_ports[recv_thread_id]);
    server_rdma_op sro;
    int ret = sro.rdma_server_init(local_ips[recv_thread_id % WORKER_N_1], local_ports[recv_thread_id], to_recv_block_mem, MEM_SIZE);

    printf("rdma_recvTd:rdma_server_init...\n");
    int*flag = (int*)(void*)to_recv_block_mem;

    size_t struct_sz = sizeof(Block);
    while (1 == 1)
    {
        struct timeval st, et;
        gettimeofday(&st, 0);
        while ((*flag) <= 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        char* real_sta_buf = to_recv_block_mem + sizeof(int);
        int total_len = (*flag);
        int* tail_total_len = (int*)(void*)(real_sta_buf + total_len);
        while ((*tail_total_len) != total_len)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        struct Block* pb = (struct Block*)(void*)real_sta_buf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        double* data_eles = (double*)(void*) (real_sta_buf + struct_sz);
        for (int i = 0; i < Pblock.ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
            if (Pblock.eles[i] > 100 || Pblock.eles[i] < -100 )
            {
                printf("P Exception!\n");
            }
        }
        printf("[%d]get pblock id=%d  ele_num=%d  isP=%d pb=%p\n", recv_thread_id,  pb->block_id, pb->ele_num, pb->isP, pb);

        size_t p_total = struct_sz + sizeof(double) * (pb->ele_num);

        struct Block* qb = (struct Block*)(void*)(real_sta_buf + p_total);

        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        printf("[%d]get qblock id=%d  ele_num=%d  isP=%d qb=%p\n", recv_thread_id,  qb->block_id, qb->ele_num, qb->isP, qb);
        //data_eles = (double*)(void*)(to_recv_block_mem + BLOCK_MEM_SZ + struct_sz);
        data_eles = (double*)(void*)(real_sta_buf + p_total + struct_sz);
        for (int i = 0; i < Qblock.ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
        }

        *flag = -1;
        *tail_total_len = -2;
        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        printf("[%d]:recv two blocks time = %lld\n", recv_thread_id, mksp);
        recv_round_robin_idx = (recv_round_robin_idx + 1) % QP_GROUP;
        hasRecved = true;
        //printf("hasRecved=%d\n", hasRecved );

    }
}