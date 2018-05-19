//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/5/2.
//  Copyright (c) 2018年 Jinkun Geng. All rights reserved.
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
#include "client_rdma_op.h"
#include "server_rdma_op.h"
using namespace std;


#define WORKER_TD 32
#define ACTION_NAME "./action"
#define STATE_NAME "./state"

char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};
std::vector<double> oldP ;
std::vector<double> oldQ ;



#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数

/*
#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数
**/

#define CAP 30
#define SEQ_LEN 2000
#define QU_LEN 10000


#define WORKER_THREAD_NUM 30

#define BLOCK_MEM_SZ (250000000)
#define MEM_SIZE (BLOCK_MEM_SZ*4)
char* to_send_block_mem;
char* to_recv_block_mem;

int GROUP_NUM = 1;
int DIM_NUM = 4;
int WORKER_NUM = 4;
int CACHE_NUM = 20;

int process_qu[WORKER_TD][SEQ_LEN];
int process_head[WORKER_TD];
int process_tail[WORKER_TD];

/*
//Jumbo
double yita = 0.002;
double theta = 0.05;
**/

//Movie-Len
double yita = 0.003;
double theta = 0.01;


vector<bool> StartCalcUpdt;

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

struct Block Pblocks[CAP];
struct Block Qblocks[CAP];

struct Updates Pupdt;
struct Updates Qupdt;
//vector<double> oldP;
//vector<double> oldQ;

struct timeval start, stop, diff;

int states[QU_LEN];
int actions[QU_LEN];

int to_send[QU_LEN];
int to_send_head, to_send_tail;

int has_recved[QU_LEN];
int recved_head, recved_tail;

int has_processed;

int disk_read_head_idx = 0;
int disk_read_tail_idx = CACHE_NUM;


std::vector<long> hash_for_row_threads[10][10][WORKER_THREAD_NUM];
std::vector<double> rates_for_row_threads[10][10][WORKER_THREAD_NUM];

std::vector<long> hash_for_col_threads[10][10][WORKER_THREAD_NUM];
std::vector<double> rates_for_col_threads[10][10][WORKER_THREAD_NUM];

//0 is to right trans Q, 1 is up, trans p

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void rdma_sendTd(int send_thread_id);
void rdma_recvTd(int recv_thread_id);

void readData(int data_thread_id);

void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);

void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
void LoadActionConfig(char* fn);
void LoadStateConfig(char* fn);
void getTestMap(map<long, double>& TestMap, int block_id);
void getBlockRates(map<long, double>& BlockMap, int block_id);
void SGD_MF();
double CalcRMSE(map<long, double>& RTestMap, Block& minP, Block& minQ);
void LoadData(int pre_read);
//void LoadData();
void CalcUpdt(int td_id);


long long time_span[2000];
int thread_id = -1;
int p_block_idx;
int q_block_idx;
int main(int argc, const char * argv[])
{
    srand(time(0));
    thread_id = atoi(argv[1]);
    WORKER_NUM = atoi(argv[2]);
    DIM_NUM = GROUP_NUM * WORKER_NUM;
    to_send_head = to_send_tail = recved_head = recved_tail = has_processed = 0;

    LoadActionConfig(ACTION_NAME);
    char state_name[100];
    sprintf(state_name, "%s-%d", state_name, thread_id);
    LoadStateConfig(state_name);
    LoadData(CACHE_NUM);
    //LoadData();
    StartCalcUpdt.resize(WORKER_THREAD_NUM);
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        StartCalcUpdt[i] = false;
    }


    std::thread data_read_thread(readData, thread_id);
    data_read_thread.detach();

    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();
    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();

    partitionP(DIM_NUM, Pblocks);
    partitionQ(DIM_NUM, Qblocks);

    for (int i = 0; i < DIM_NUM; i++)
    {
        for (int j = 0; j < Pblocks[i].ele_num; j++)
        {
            Pblocks[i].eles[j] = drand48() * 0.6;
            Qblocks[i].eles[j] = drand48() * 0.6;
            //0.3
            //Pblocks[i].eles[j] = drand48() * 0.3;
            //Qblocks[i].eles[j] = drand48() * 0.3;
        }
    }


    std::vector<thread> td_vec;
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec.push_back(std::thread(CalcUpdt, i));
    }
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec[i].detach();
        //printf("%d  has detached\n", i );
    }


    int block_to_process;
    int action_to_process;
    int action = 0;
    int state_idx = 0;
    std::vector<int> p_to_process(GROUP_NUM);
    std::vector<int> q_to_process(GROUP_NUM);
    std::vector<bool> send_this_p(GROUP_NUM);
    //Init Mark
    int iter_cnt = 0;
    struct timeval st, et, tspan;
    long long mksp;
    while (1 == 1)
    {
        if (iter_cnt == 0)
        {
            gettimeofday(&st, 0);
        }
        else
        {
            if (iter_cnt % 10 == 0)
            {
                gettimeofday(&et, 0);

                mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
                //if (iter_cnt % 100 == 0)
                printf("hehere %d\t%lld\n", iter_cnt, mksp);
                time_span[iter_cnt / 10] = mksp;
            }
            if (iter_cnt == 1200)
            {
                for (int i = 0; i < 120; i++)
                {
                    printf("%lld\n", time_span[i]);
                }
            }
        }
        for (int i = 0; i < GROUP_NUM; i++)
        {
            block_to_process = states[state_idx];
            action = actions[state_idx];
            //printf("block_to_process %d\n", block_to_process );
            p_to_process[i] = block_to_process / (DIM_NUM);
            q_to_process[i] = block_to_process % (DIM_NUM);
            //printf("block_to_process %d  p %d q %d\n", block_to_process, p_to_process[i], q_to_process[i]  );
            //0 is to right trans Q, 1 is up, trans p
            if (action == 0)
            {
                send_this_p[i] = false;
            }
            else
            {
                send_this_p[i] = true;
            }
            state_idx++;
        }




        for (int i = 0; i < GROUP_NUM; i++)
        {

            p_block_idx = p_to_process[i];
            q_block_idx = q_to_process[i];

            SGD_MF();


            if (iter_cnt % 10 == 0)
            {
                //WriteLog(Pblocks[p_block_idx], Qblocks[q_block_idx], iter_cnt);
            }

            //patch
            /*
                        if (thread_id != WORKER_NUM - 1)
                        {
                            to_send_tail = (to_send_tail + 1) % QU_LEN;
                        }
            **/

            to_send_tail = (to_send_tail + 1) % QU_LEN;

            //patch the two above mutual
            has_processed++;
            printf("processed success has_processed=%d\n", has_processed );
            while (has_processed > recved_head || has_processed >= disk_read_tail_idx)
            {
                //Wait
                //printf("to recv has_processed=%d recved_head=%d disk_read_tail_idx=%d\n", has_processed, recved_head, disk_read_tail_idx);
                //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }


        }

        //patch
        /*
                if (thread_id == WORKER_NUM - 1)
                {
                    to_send_tail =  (to_send_tail + 2) % QU_LEN;
                }
        **/


        iter_cnt++;

        printf("iterddd %d\n", iter_cnt );

        if (iter_cnt == 2000)
        {
            printf("iter_cnt=%d\n", iter_cnt );
            //exit(0);
        }

        //printf("Processing has_processed=%d\n", has_processed );
    }
}


void CalcUpdt(int td_id)
{

    while (1 == 1)
    {

        if (StartCalcUpdt[td_id])
        {
            //printf("enter CalcUpdt\n");
            int times_thresh = 100;
            int row_sta_idx = Pblocks[p_block_idx].sta_idx;
            int col_sta_idx = Qblocks[q_block_idx].sta_idx;
            size_t rtsz;
            size_t ctsz;
            rtsz = hash_for_row_threads[p_block_idx][q_block_idx][td_id].size();
            ctsz = hash_for_col_threads[p_block_idx][q_block_idx][td_id].size();
            if (rtsz == 0 || ctsz == 0)
            {
                printf("empty p %d q %d\n", p_block_idx, q_block_idx );
                StartCalcUpdt[td_id] = false;
                continue;
                //exit(0);
            }
            int rand_idx = -1;
            while (times_thresh--)
            {
                //printf("times_thresh=%d\n", times_thresh );
                rand_idx = random() % rtsz;
                long real_hash_idx = hash_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                long i = real_hash_idx / M - row_sta_idx;
                long j = real_hash_idx % M - col_sta_idx;
                double error = rates_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                if (i < 0 || j < 0 || i >= Pblocks[p_block_idx].height || j >= Qblocks[q_block_idx].height)
                {
                    printf("[%d] continue l \n", td_id);
                    continue;
                }
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Pblocks[p_block_idx].eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                    if (Pblocks[p_block_idx].eles[i * K + k] + 1 == Pblocks[p_block_idx].eles[i * K + k] - 1)
                    {
                        printf("p %d q %d  error =%lf i=%d j=%d k=%d rand_idx=%d vale=%lf pvale=%lf  qvalue=%lf\n", p_block_idx, q_block_idx, error, i, j, k, rand_idx,  rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx], oldP[i * K + k], oldQ[j * K + k] );
                        getchar();
                    }
                }

                rand_idx = random() % ctsz;
                real_hash_idx = hash_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                if (i < 0 || j < 0 || i >= Pblocks[p_block_idx].height || j >= Qblocks[q_block_idx].height)
                {
                    printf("[%d] continue l11 \n", td_id);
                    continue;
                }
                error = rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];

                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Qblocks[q_block_idx].eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
                    if (Qblocks[q_block_idx].eles[j * K + k] + 1 == Qblocks[q_block_idx].eles[j * K + k] - 1)
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

void CalcUpdt1(int td_id)
{


    while (1 == 1)
    {
        if (StartCalcUpdt[td_id])
        {


            int times_thresh = 200;
            int row_sta_idx = Pblocks[p_block_idx].sta_idx;
            int col_sta_idx = Qblocks[q_block_idx].sta_idx;
            size_t rtsz;
            size_t ctsz;
            rtsz = hash_for_row_threads[p_block_idx][q_block_idx][td_id].size();
            ctsz = hash_for_col_threads[p_block_idx][q_block_idx][td_id].size();
            if (rtsz == 0)
            {
                //printf(" rtsz=0 p %d q %d td_id=%d\n", p_block_idx, q_block_idx, td_id );
                StartCalcUpdt[td_id] = false;

                continue;
            }
            int rand_idx = -1;
            int cnt = 0;
            long real_hash_idx;
            long i ;
            long j ;
            double error;
            srand(time(0));
            while (cnt < times_thresh)
            {

                rand_idx = random() % rtsz;
                //printf("cnt = %d rand_idx=%d\n", cnt, rand_idx);
                real_hash_idx = hash_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                error = rates_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                if (i < 0 || j < 0 || i >= Pblocks[p_block_idx].height || j >= Qblocks[q_block_idx].height)
                {
                    //printf("[%d] continue l [%d][%d] pq [%ld][%ld]  %ld\n", td_id, p_block_idx, q_block_idx, i, j, real_hash_idx);
                    cnt++;
                    //getchar();
                    //exit(0);
                    continue;
                }
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Pblocks[p_block_idx].eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                    if (Pblocks[p_block_idx].eles[i * K + k] + 1 == Pblocks[p_block_idx].eles[i * K + k] - 1)
                    {
                        printf("p %d q %d  error =%lf i=%d j=%d k=%d rand_idx=%d vale=%lf pvale=%lf  qvalue=%lf\n", p_block_idx, q_block_idx, error, i, j, k, rand_idx,  rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx], oldP[i * K + k], oldQ[j * K + k] );
                        getchar();
                    }
                }
                cnt++;
            }
            cnt = 0;
            while (cnt < times_thresh)
            {
                rand_idx = random() % ctsz;
                real_hash_idx = hash_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                if (i < 0 || j < 0 || i >= Pblocks[p_block_idx].height || j >= Qblocks[q_block_idx].height)
                {
                    //printf("[%d] c11ontinue l [%d][%d] pq [%ld][%ld]  %ld\n", td_id, p_block_idx, q_block_idx, i, j, real_hash_idx);
                    //getchar();
                    cnt++;
                    //getchar();
                    //exit(0);
                    continue;
                }
                error = rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Qblocks[q_block_idx].eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
                    if (Qblocks[q_block_idx].eles[j * K + k] + 1 == Qblocks[q_block_idx].eles[j * K + k] - 1)
                    {
                        printf("p %d q %d  error =%lf i=%d j=%d k=%d rand_idx=%d vale=%lf pvale=%lf  qvalue=%lf\n", p_block_idx, q_block_idx, error, i, j, k, rand_idx,  rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx], oldP[i * K + k], oldQ[j * K + k] );
                        getchar();

                    }
                }
                cnt++;

            }

            StartCalcUpdt[td_id] = false;
            //printf("finish %d  %ld %ld\n",  td_id, rtsz, ctsz);

        }
    }


}


void LoadActionConfig(char* fn)
{
    int loc = 0;
    for (int i = 0; i < SEQ_LEN; i++ )
    {
        for (int gp = 0; gp < GROUP_NUM; gp++)
        {
            loc = i * GROUP_NUM + gp;

            actions[loc] = gp % 2;

            //only one direction  patch
            //actions[loc] = 0;

        }
    }

}
void LoadStateConfig(char* fn)
{

    for (int gp = 0; gp < GROUP_NUM; gp++)
    {
        int row = thread_id * GROUP_NUM + gp;
        int col = DIM_NUM - 1 - ( thread_id * GROUP_NUM + gp);
        states[gp] = row * DIM_NUM + col;
        printf("state[%d] %d\n", gp, states[gp] );
    }


    //right patch
    /*
        for (int gp = 0; gp < GROUP_NUM; gp++)
        {
            int row = thread_id  + gp * WORKER_NUM;
            int col = DIM_NUM - 1 - row;
            states[gp] = row * DIM_NUM + col;
        }
    **/


    for (size_t i = 0; i < SEQ_LEN; i++ )
    {
        for (int gp = 0 ; gp < GROUP_NUM; gp++)
        {
            //0 is to right ,send Q and will  recv Q; 1 is up, send p and will  recv P
            int loc = i * GROUP_NUM + gp;
            //printf("loc [%d] act %d\n", loc, actions[loc]);
            if (actions[loc] == 0)
            {

                to_send[loc] = states[loc] % DIM_NUM;
                has_recved[loc] = (to_send[loc] + GROUP_NUM) % DIM_NUM;

                states[loc + GROUP_NUM] = (states[loc] / DIM_NUM) * DIM_NUM + ((states[loc] + GROUP_NUM) % DIM_NUM);



                //patch
                /*
                to_send[loc] = states[loc] % DIM_NUM;
                has_recved[loc] = (to_send[loc] + 1) % DIM_NUM;

                states[loc + GROUP_NUM] = (states[loc] / DIM_NUM) * DIM_NUM + ((states[loc] + 1) % DIM_NUM);
                **/


            }
            else
            {
                to_send[loc] = states[loc] / DIM_NUM;
                has_recved[loc] = (to_send[loc]  + DIM_NUM - GROUP_NUM) % DIM_NUM;

                states[loc + GROUP_NUM] = ((states[loc] / DIM_NUM + DIM_NUM - GROUP_NUM) % DIM_NUM) * DIM_NUM + (states[loc] % DIM_NUM);
            }

            //patch
            /*
                        if (thread_id == WORKER_NUM - 1)
                        {
                            if (gp == 1)
                            {
                                int tmp = states[loc];
                                states[loc] = states[loc - 1];
                                states[loc - 1] = tmp;
                                tmp = to_send[loc] ;
                                to_send[loc] = to_send[loc - 1];
                                to_send[loc - 1] = tmp;

                            }

                        }
            **/

            //
        }

    }
    for (int i = 0; i < 100; i++)
    {
        printf("%d\t", actions[i]);
    }
    printf("\n");
    for (int i = 0; i < 100; i++)
    {
        printf("%d\t", states[i]);
    }
    printf("\n");
    //exit(0);


}
void LoadData(int pre_read)
{
    char fn[100];
    long hash_id = -1;
    double rate;
    long cnt = 0;
    for (int i = 0; i < pre_read; i++)
    {
        int data_idx = states[i];
        int row = data_idx / DIM_NUM;
        int col = data_idx % DIM_NUM;
        int phy_row = row * (2 / GROUP_NUM);
        int phy_col = col * (2 / GROUP_NUM);
        if (hash_for_row_threads[row][col][0].size() != 0)
        {
            continue;
        }
        for (int row_sta = phy_row; row_sta < phy_row + (2 / GROUP_NUM); row_sta++)
        {
            for (int col_sta = phy_col; col_sta < phy_col  + (2 / GROUP_NUM); col_sta++)
            {
                data_idx = row_sta * DIM_NUM * (2 / GROUP_NUM) + col_sta;
                sprintf(fn, "%s%d", FILE_NAME, data_idx);
                printf("fn=%s  :[%d][%d]\n", fn, row_sta, col_sta );
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

                    //if (row == 5 && col == 2)
                    //printf("row=%d col=%d rr=%ld cc=%ld\n", row, col, ((hash_id) / M), ((hash_id) % M)  );
                    //break;

                }
                //printf("row=%d col=%d sz =%ld\n", row, col, hash_for_row_threads[row][col][0].size() );
            }
        }


    }
}

void LoadData2()
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
        //row /= 2;
        //col /= 2;
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
}
void readData(int data_thread_id)
{

    char fn[100];
    long hash_id;
    double rate;
    long cnt = 0;

    while (1 == 1)
    {
        //printf("head_idx=%d  to_send_tail=%d tail_idx=%d\n", head_idx, to_send_tail, tail_idx );

        if (disk_read_tail_idx >= QU_LEN)
        {
            //printf("break\n");
            break;
        }
        if (disk_read_head_idx >= has_processed)
        {
            //printf("head>=has_processed  %d  %d\n", disk_read_head_idx, has_processed);
            //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        int data_idx = states[disk_read_tail_idx];
        int row = data_idx / DIM_NUM;
        int col = data_idx % DIM_NUM;

        //printf("fff 5 2 %ld\n", hash_for_row_threads[5][2][0].size() );
        //if (TrainMaps[row][col].size() == 0)
        if (hash_for_row_threads[row][col][0].size() == 0)
        {
            int phy_row = row * (2 / GROUP_NUM);
            int phy_col = col * (2 / GROUP_NUM);
            for (int row_sta = phy_row; row_sta < phy_row + (2 / GROUP_NUM); row_sta++)
            {
                for (int col_sta = phy_col; col_sta < phy_col  + (2 / GROUP_NUM); col_sta++)
                {

                    data_idx = row_sta * DIM_NUM * (2 / GROUP_NUM) + col_sta;
                    sprintf(fn, "%s%d", FILE_NAME, data_idx);
                    //printf("read fn =%s\n", fn );
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
            }
        }
        //printf("read [%d][%d]\n", row, col  );

        disk_read_tail_idx++;
        data_idx = states[disk_read_head_idx];
        row = data_idx / DIM_NUM;
        col = data_idx % DIM_NUM;
        //TrainMaps[row][col].clear();
        //TestMaps[row][col].clear();
        /*
        for (int kk = 0; kk < WORKER_THREAD_NUM; kk++)
        {
            hash_for_row_threads[row][col][kk].clear();
            rates_for_row_threads[row][col][kk].clear();
            hash_for_col_threads[row][col][kk].clear();
            rates_for_col_threads[row][col][kk].clear();
        }
        printf("free [%d][%d]\n", row, col );
        **/
        disk_read_head_idx++;


    }
    printf("Exit read data\n");

}


void WriteLog(Block & Pb, Block & Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./Rtrack/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    printf("fn:%s\n", fn );
    sprintf(fn, "./Rtrack/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
    printf("fn:%s\n", fn );
    //getchar();
}




void SGD_MF()
{

    struct timeval beg, ed;
    long long mksp;

    oldP = Pblocks[p_block_idx].eles;
    oldQ = Qblocks[q_block_idx].eles;
    for (int i = 0; i < Pblocks[p_block_idx].ele_num; i++)
    {
        if (oldP[i] > 100 || oldP[i] < -100)
        {
            printf("P Exception! [%d] %lf\n", i, oldP[i]);
            getchar();
        }

    }
    printf("comere hhe\n");
    for (int i = 0; i < Qblocks[q_block_idx].ele_num; i++)
    {
        if (oldQ[i] > 100 || oldQ[i] < -100)
        {
            printf("Q Exception! [%d] %lf\n", i, oldQ[i]);
            getchar();
        }

    }



    {

        gettimeofday(&beg, 0);
        for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
        {
            StartCalcUpdt[ii] = true;
        }

        //printf("check 5 2 %ld\n", hash_for_row_threads[5][2][0].size() );


        bool canbreak = true;
        while (1 == 1)
        {
            canbreak = true;
            for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
            {

                if (StartCalcUpdt[ii])
                {
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
        printf(" SGD time = %lld upt p %d q %d\n", mksp, p_block_idx, q_block_idx);


    }


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
    printf("get connection from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    int right_idx = (send_thread_id + 1) % WORKER_NUM;
    char* remote_ip = local_ips[right_idx];
    int remote_port = local_ports[right_idx];

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
        printf("Trying to connect to %s %d\n", remote_ip, remote_port);
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    int send_cnt = 0;
    //getchar();
    while (1 == 1)
    {
        //printf("to_send_head=%d to_send_tail=%d\n", to_send_head, to_send_tail );
        if (to_send_head < to_send_tail)
        {
            //printf("come here send\n");
            //getchar();
            int block_idx = to_send[to_send_head];
            int block_p_or_q = actions[to_send_head];
            //0 is to right trans Q, 1 is up, trans p
            size_t struct_sz = sizeof(Block);
            size_t data_sz = 0;
            char*buf = NULL;
            if (block_p_or_q == 0)
            {
                //send q
                data_sz = sizeof(double) * Qblocks[block_idx].eles.size();
                //printf("to_send_head =%d send q block_idx=%d realid %d\n", to_send_head, block_idx, Qblocks[block_idx].block_id);
                buf = (char*)malloc(struct_sz + data_sz);
                //getchar();
                //printf("before memcpy1\n");
                memcpy(buf, &(Qblocks[block_idx]), struct_sz);
                //printf("before memcpy2\n");
                memcpy(buf + struct_sz, (char*) & (Qblocks[block_idx].eles[0]), data_sz);
            }
            else
            {
                //send p
                data_sz = sizeof(double) * Pblocks[block_idx].eles.size();
                //printf("to_send_head =%d send p block_idx=%d realid %d\n", to_send_head, block_idx, Pblocks[block_idx].block_id);
                buf = (char*)malloc(struct_sz + data_sz);
                memcpy(buf, &(Pblocks[block_idx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Pblocks[block_idx].eles[0]), data_sz);
            }
            //printf("before send... stucsz=%ld data_sz=%ld \n", struct_sz, data_sz);
            size_t total_len = struct_sz + data_sz;
            size_t sent_len = 0;
            size_t remain_len = total_len;
            int ret = -1;
            size_t to_send_len = 4096;

            struct timeval st, et, tspan;
            gettimeofday(&st, 0);

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
            }

            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;

            printf("[Id:%d] send success stucsz=%ld data_sz=%ld %d block_id=%d timespan=%lld to_Send_head=%d\n", thread_id, struct_sz, data_sz, ret, block_idx, mksp, to_send_head);

            //getchar();
            //printf("before free..\n");
            //getchar();
            free(buf);
            //printf("after free...\n");
            to_send_head = (to_send_head + 1) % QU_LEN;
            //getchar();
        }
    }

}
void recvTd(int recv_thread_id)
{

    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    if (connfd < 0)
    {
        std::cout << "accept error";
        exit(-1);
    }
    printf("[Td:%d] worker get connection  connfd=%d\n", recv_thread_id, connfd);
    int cnt = 0;
    size_t expected_len = sizeof(Block);
    char* blockBuf = NULL;
    char* dataBuf = NULL;
    size_t cur_len = 0;
    int ret = 0;
    while (1 == 1)
    {
        //if (recved_head < recved_tail)
        {

            int block_idx = has_recved[recved_head];
            int block_p_or_q = actions[recved_head];
            //printf("recved_head=%d block_idx=%d  block_p_or_q=%d\n", recved_head, block_idx, block_p_or_q );
            //0 is to right trans/recv Q, 1 is up, trans p
            cur_len = 0;
            ret = 0;
            blockBuf = (char*)malloc(sizeof(Block));

            struct timeval st, et, tspan;


            while (cur_len < expected_len)
            {
                //printf("before recv...\n");
                ret = recv(connfd, blockBuf + cur_len, expected_len - cur_len, 0);
                //printf("ret = %d cur_len=%ld expected_len=%ld\n", ret, cur_len, expected_len);
                if (ret < 0)
                {
                    printf("Mimatch! error=%d\n", errno);
                }
                //getchar();
                cur_len += ret;
            }
            struct Block* pb = (struct Block*)(void*)blockBuf;
            size_t data_sz = sizeof(double) * (pb->ele_num);
            char* dataBuf = (char*)malloc(data_sz);

            cur_len = 0;
            ret = 0;
            gettimeofday(&st, 0);
            while (cur_len < data_sz)
            {
                ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
                if (ret < 0)
                {
                    printf("Mimatch!\n");
                }
                cur_len += ret;
            }
            double* data_eles = (double*)(void*)dataBuf;
            //printf("tofill bid=%d real id %d\n", block_idx, pb->block_id );

            if (block_p_or_q == 0)
            {
                //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
                // recv q
                Qblocks[block_idx].block_id = pb->block_id;
                Qblocks[block_idx].sta_idx = pb->sta_idx;
                Qblocks[block_idx].height = pb->height;
                Qblocks[block_idx].ele_num = pb->ele_num;
                //printf("recvQ pb->ele_num=%ld qbsz=%ld\n", pb->ele_num, Qblocks[block_idx].eles.size() );
                Qblocks[block_idx].eles.clear();
                //printf("recvQ  qbsz=%ld\n", Qblocks[block_idx].eles.size() );
                Qblocks[block_idx].eles.resize(pb->ele_num);
                //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
                Qblocks[block_idx].isP = pb->isP;
                for (int i = 0; i < pb->ele_num; i++)
                {
                    //printf("i=%d\n", i );
                    Qblocks[block_idx].eles[i] = data_eles[i];
                }
                //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
            }
            else
            {
                Pblocks[block_idx].block_id = pb->block_id;
                Pblocks[block_idx].sta_idx = pb->sta_idx;
                Pblocks[block_idx].height = pb->height;
                Pblocks[block_idx].ele_num = pb->ele_num;
                Pblocks[block_idx].eles.resize(pb->ele_num);
                Pblocks[block_idx].isP = pb->isP;
                for (int i = 0; i < pb->ele_num; i++)
                {
                    Pblocks[block_idx].eles[i] = data_eles[i];
                }
            }
            //printf("recv complete\n");
            //getchar();
            //printf("before free blockBuf\n");
            //getchar();
            free(blockBuf);
            //printf("before free dataBuf\n");
            free(dataBuf);
            //printf("after free two\n");

            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("recv success time = %lld, recved_head=%d has_processed=%d data_sz=%ld\n", mksp, recved_head, has_processed, data_sz );

            recved_head = (recved_head + 1) % QU_LEN;
        }
    }

}

void partitionP(int portion_num,  Block * Pblocks)
{
    int i = 0;
    int height = N / portion_num;
    int last_height = N - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Pblocks[i].block_id = i;
        Pblocks[i].data_age = 0;
        Pblocks[i].eles.clear();
        Pblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Pblocks[i].height = last_height;
        }
        Pblocks[i].sta_idx = sta_idx;
        printf("i-%d sta_idx-%d\n", i, sta_idx );
        Pblocks[i].ele_num = Pblocks[i].height * K;
        Pblocks[i].eles.resize(Pblocks[i].ele_num);
    }

}

void partitionQ(int portion_num,  Block * Qblocks)
{
    int i = 0;
    int height = M / portion_num;
    int last_height = M - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Qblocks[i].block_id = i;
        Qblocks[i].data_age = 0;
        Qblocks[i].eles.clear();
        Qblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Qblocks[i].height = last_height;
        }
        Qblocks[i].sta_idx = sta_idx;
        Qblocks[i].ele_num = Qblocks[i].height * K;
        Qblocks[i].eles.resize(Qblocks[i].ele_num);

    }

}




void rdma_sendTd(int send_thread_id)
{
    printf("worker send_thread_id=%d\n", send_thread_id);
    printf("worker send waiting for 3s...\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    char* remote_ip = remote_ips[send_thread_id];
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
    size_t offset = 0;
    while (1 == 1)
    {
        if (to_send_head < to_send_tail)
        {

            int block_idx = to_send[to_send_head];
            int block_p_or_q = actions[to_send_head];
            //0 is to right trans Q, 1 is up, trans p
            size_t struct_sz = sizeof(Block);
            size_t data_sz = 0;
            char*buf = to_send_block_mem;
            if (block_p_or_q == 0)
            {
                //send q
                data_sz = sizeof(double) * Qblocks[block_idx].eles.size();
                //printf("to_send_head =%d send q block_idx=%d realid %d\n", to_send_head, block_idx, Qblocks[block_idx].block_id);
                size_t total_len = struct_sz + data_sz;
                memcpy(buf, &(Qblocks[block_idx]), struct_sz);
                //printf("before memcpy2\n");
                memcpy(buf + struct_sz, (char*) & (Qblocks[block_idx].eles[0]), data_sz);
                ret = cro.start_remote_write(total_len, offset);
                printf("writer one Pblock\n");
            }
            else
            {
                //send p
                data_sz = sizeof(double) * Pblocks[block_idx].eles.size();

                size_t total_len = struct_sz + data_sz;
                memcpy(buf, &(Pblocks[block_idx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Pblocks[block_idx].eles[0]), data_sz);
                ret = cro.start_remote_write(total_len, offset);
                printf("writer one Qblock\n");
            }
            offset = (offset + BLOCK_MEM_SZ) % MEM_SIZE;
            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;

            printf("[Id:%d] send success stucsz=%ld data_sz=%ld %d block_id=%d timespan=%lld to_Send_head=%d\n", thread_id, struct_sz, data_sz, ret, block_idx, mksp, to_send_head);

            to_send_head = (to_send_head + 1) % QU_LEN;
        }
    }

}
void rdma_recvTd(int recv_thread_id)
{
    printf("rdma_recv thread_id = %d\n local_ip=%s  local_port=%d\n", recv_thread_id, local_ips[recv_thread_id], local_ports[recv_thread_id]);
    server_rdma_op sro;
    int ret = sro.rdma_server_init(local_ips[recv_thread_id], local_ports[recv_thread_id], to_recv_block_mem, MEM_SIZE);

    printf("rdma_recvTd:rdma_server_init...\n");
    size_t struct_sz = sizeof(Block);
    size_t offset = 0;

    while (1 == 1)
    {

        int block_idx = has_recved[recved_head];
        int block_p_or_q = actions[recved_head];
        //printf("recved_head=%d block_idx=%d  block_p_or_q=%d\n", recved_head, block_idx, block_p_or_q );
        //0 is to right trans/recv Q, 1 is up, trans p
        struct timeval st, et, tspan;
        char* buf = to_recv_block_mem + offset;
        struct Block* pb = (struct Block*)(void*)buf;
        while (pb->block_id < 0)
        {
            //printf("waiting... block_id = %d\n", pb->block_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        gettimeofday(&st, 0);
        struct Block* pb = (struct Block*)(void*)blockBuf;
        size_t data_sz = sizeof(double) * (pb->ele_num);
        char* dataBuf = buf + struct_sz;

        if (block_p_or_q == 0)
        {
            //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
            // recv q
            Qblocks[block_idx].block_id = pb->block_id;
            Qblocks[block_idx].sta_idx = pb->sta_idx;
            Qblocks[block_idx].height = pb->height;
            Qblocks[block_idx].ele_num = pb->ele_num;
            Qblocks[block_idx].eles.clear();
            Qblocks[block_idx].eles.resize(pb->ele_num);
            //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
            Qblocks[block_idx].isP = pb->isP;
            for (int i = 0; i < pb->ele_num; i++)
            {
                Qblocks[block_idx].eles[i] = data_eles[i];
            }
            //printf("recvQ pb->ele_num=%ld\n", pb->ele_num);
        }
        else
        {
            Pblocks[block_idx].block_id = pb->block_id;
            Pblocks[block_idx].sta_idx = pb->sta_idx;
            Pblocks[block_idx].height = pb->height;
            Pblocks[block_idx].ele_num = pb->ele_num;
            Pblocks[block_idx].eles.resize(pb->ele_num);
            Pblocks[block_idx].isP = pb->isP;
            for (int i = 0; i < pb->ele_num; i++)
            {
                Pblocks[block_idx].eles[i] = data_eles[i];
            }
        }
        pb->block_id = -1;
        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        printf("recv success time = %lld, recved_head=%d has_processed=%d data_sz=%ld\n", mksp, recved_head, has_processed, data_sz );
        offset = (offset + BLOCK_MEM_SZ) % (MEM_SIZE);
        recved_head = (recved_head + 1) % QU_LEN;

    }

}