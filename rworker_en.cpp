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
using namespace std;


#define WORKER_TD 32
#define ACTION_NAME "./action"
#define STATE_NAME "./state"

char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};


//#define FILE_NAME "./traina.txt"
//#define TEST_NAME "./testa.txt"

#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数

//#define N 71567
//#define M 65133
//#define K  40 //主题个数
#define CAP 30
#define SEQ_LEN 1000
#define QU_LEN 5000
#define ThreshIter 100


#define WORKER_THREAD_NUM 30

int GROUP_NUM = 1;
int DIM_NUM = 4;
int WORKER_NUM = 2;
int CACHE_NUM = 20;

int process_qu[WORKER_TD][SEQ_LEN];
int process_head[WORKER_TD];
int process_tail[WORKER_TD];

double yita = 0.002;
double theta = 0.05;
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
void CalcUpdt(int td_id);

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
            Pblocks[i].eles[j] = drand48() * 0.3;
            Qblocks[i].eles[j] = drand48() * 0.3;
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
    while (1 == 1)
    {
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

            struct timeval st, et, tspan;
            gettimeofday(&st, 0);
            SGD_MF();
            gettimeofday(&et, 0);

            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("calc time = %lld\n", mksp);

            to_send_tail = (to_send_tail + 1) % QU_LEN;
            has_processed++;
            printf("processed success has_processed=%d\n", has_processed );
            while (has_processed > recved_head || has_processed >= disk_read_tail_idx)
            {
                //Wait
                //printf("to recv has_processed=%d recved_head=%d disk_read_tail_idx=%d\n", has_processed, recved_head, disk_read_tail_idx);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }


        }
        iter_cnt++;
        if (iter_cnt == 900)
        {
            printf("iter_cnt=%d\n", iter_cnt );
            exit(0);
        }

        printf("Processing has_processed=%d\n", has_processed );
    }
}

void CalcUpdt(int td_id)
{
    std::vector<double> Pvec(K);
    std::vector<double> Qvec(K);
    while (1 == 1)
    {
        if (StartCalcUpdt[td_id])
        {
            int times_thresh = 100;
            int row_sta_idx = Pblocks[p_block_idx].sta_idx;
            int col_sta_idx = Qblocks[q_block_idx].sta_idx;
            size_t rtsz;
            size_t ctsz;
            rtsz = hash_for_row_threads[p_block_idx][q_block_idx][td_id].size();
            ctsz = hash_for_col_threads[p_block_idx][q_block_idx][td_id].size();
            if (rtsz == 0)
            {
                printf("p %d q %d\n", p_block_idx, q_block_idx );
                exit(0);
            }
            int rand_idx = -1;
            while (times_thresh--)
            {
                rand_idx = random() % rtsz;
                long real_hash_idx = hash_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                long i = real_hash_idx / M - row_sta_idx;
                long j = real_hash_idx % M - col_sta_idx;
                double error = rates_for_row_threads[p_block_idx][q_block_idx][td_id][rand_idx];

                for (int k = 0; k < K; ++k)
                {
                    //error -= oldP[i * K + k] * oldQ[j * K + k];
                    Pvec[k] = Pblocks[p_block_idx].eles[i * K + k];
                    Qvec[k] = Qblocks[q_block_idx].eles[j * K + k];
                    if (i * K + k >= Pblocks[p_block_idx].eles.size())
                    {
                        printf("i=%d K=%d k=%d sum=%d sz=%ld p_block_idx=%d q_block_idx=%d, rr=%ld cc=%ld row_sta_idx=%ld col_sta_idx=%ld\n", i, K, k, i * K + k, Pblocks[p_block_idx].eles.size(), p_block_idx, q_block_idx, real_hash_idx / M, real_hash_idx % M, row_sta_idx, col_sta_idx );
                        exit(1);
                    }
                    if (j * K + k >= Qblocks[q_block_idx].eles.size()  )
                    {
                        printf("j=%d K=%d k=%d sum=%d sz=%ld\n", j, K, k, j * K + k, Qblocks[q_block_idx].eles.size());
                        exit(1);
                    }
                    error -= Pvec[k] * Qvec[k];
                }
                for (int k = 0; k < K; ++k)
                {
                    //Pblocks[p_block_idx].eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                    Pblocks[p_block_idx].eles[k] += yita * (error * Qvec[k] - theta * Pvec[k]);

                }

                rand_idx = random() % ctsz;
                real_hash_idx = hash_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                i = real_hash_idx / M - row_sta_idx;
                j = real_hash_idx % M - col_sta_idx;
                error = rates_for_col_threads[p_block_idx][q_block_idx][td_id][rand_idx];
                for (int k = 0; k < K; ++k)
                {
                    //error -= oldP[i * K + k] * oldQ[j * K + k];
                    Pvec[k] = Pblocks[p_block_idx].eles[i * K + k];
                    Qvec[k] = Qblocks[q_block_idx].eles[j * K + k];
                    error -= Pvec[k] * Qvec[k];
                }
                for (int k = 0; k < K; ++k)
                {
                    //Qblocks[q_block_idx].eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);
                    Qblocks[q_block_idx].eles[k] += yita * (error * Pvec[k] - theta * Qvec[k]);
                }
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
            }
            else
            {
                to_send[loc] = states[loc] / DIM_NUM;
                has_recved[loc] = (to_send[loc]  + DIM_NUM - GROUP_NUM) % DIM_NUM;

                states[loc + GROUP_NUM] = ((states[loc] / DIM_NUM + DIM_NUM - GROUP_NUM) % DIM_NUM) * DIM_NUM + (states[loc] % DIM_NUM);
            }
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


}
void LoadData(int pre_read)
{
    char fn[100];
    long hash_id;
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
                //if (TrainMaps[row][col].size() != 0)
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

                while (!ifs.eof())
                {
                    ifs >> hash_id >> rate;
                    //TrainMaps[row][col].insert(pair<long, double>(hash_id, rate));

                    ridx = ((hash_id) / M) % WORKER_THREAD_NUM;
                    cidx = ((hash_id) % M) % WORKER_THREAD_NUM;

                    hash_for_row_threads[row][col][ridx].push_back(hash_id);
                    rates_for_row_threads[row][col][ridx].push_back(rate);
                    hash_for_col_threads[row][col][cidx].push_back(hash_id);
                    rates_for_col_threads[row][col][cidx].push_back(rate);
                    //printf("row=%d col=%d rr=%ld cc=%ld\n", row, col, ((hash_id) / M), ((hash_id) % M)  );
                    //break;

                }
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
                    while (!ifs.eof())
                    {
                        ifs >> hash_id >> rate;
                        //TrainMaps[row][col].insert(pair<long, double>(hash_id, rate));

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
        printf("read [%d][%d]\n", row, col  );

        disk_read_tail_idx++;
        data_idx = states[disk_read_head_idx];
        row = data_idx / DIM_NUM;
        col = data_idx % DIM_NUM;
        //TrainMaps[row][col].clear();
        //TestMaps[row][col].clear();
        for (int kk = 0; kk < WORKER_THREAD_NUM; kk++)
        {
            hash_for_row_threads[row][col][kk].clear();
            rates_for_row_threads[row][col][kk].clear();
            hash_for_col_threads[row][col][kk].clear();
            rates_for_col_threads[row][col][kk].clear();
        }
        printf("free [%d][%d]\n", row, col );
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




double CalcRMSE(map<long, double>& RTestMap, Block & minP, Block & minQ)
{
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    long row_sta_idx = minP.sta_idx;
    long col_sta_idx = minQ.sta_idx;
    //printf("Psz = %ld  Qsz= %ld row_sta_idx=%ld, col_sta_idx=%ld\n", minP.ele_num, minQ.ele_num, row_sta_idx, col_sta_idx );
    for (iter = RTestMap.begin(); iter != RTestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M - row_sta_idx;
        long col_idx = real_hash_idx % M - col_sta_idx;
        double sum = 0;

        for (int k = 0; k < K; k++)
        {
            sum += minP.eles[row_idx * K + k] * minQ.eles[col_idx * K + k];
        }

        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }
    if (cnt != 0)
    {
        rmse /= cnt;
        rmse = sqrt(rmse);
    }
    else
    {
        rmse = 0;
    }
    return rmse;
}


void SGD_MF()
{
    double error = 0;
    int row_sta_idx = Pblocks[p_block_idx].sta_idx;
    int col_sta_idx = Qblocks[q_block_idx].sta_idx;
    int row_len = Pblocks[p_block_idx].height;
    int col_len = Qblocks[q_block_idx].height;
    int Psz =  Pblocks[p_block_idx].height * K;
    int Qsz = Qblocks[q_block_idx].height * K;

    int iter_cnt = 0;
    int update_num = 0;
    struct timeval beg, ed;
    long long mksp;
    memset(&beg, 0, sizeof(struct timeval));
    memset(&ed, 0, sizeof(struct timeval));
    {

        gettimeofday(&beg, 0);
        for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
        {
            StartCalcUpdt[ii] = true;
        }

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
        printf(" SGD time = %lld\n", mksp);
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
        //printf("Trying to connect to %s %d\n", remote_ip, remote_port);
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    int send_cnt = 0;

    while (1 == 1)
    {
        //printf("to_send_head=%d to_send_tail=%d\n", to_send_head, to_send_tail );
        if (to_send_head < to_send_tail)
        {
            //printf("come here send\n");
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
                memcpy(buf, &(Qblocks[block_idx]), struct_sz);
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

            printf("[Id:%d] send success stucsz=%ld data_sz=%ld %d timespan=%lld to_Send_head=%d\n", thread_id, struct_sz, data_sz, ret, mksp, to_send_head);
            //getchar();
            free(buf);

            to_send_head = (to_send_head + 1) % QU_LEN;

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
                // recv q
                Qblocks[block_idx].block_id = pb->block_id;
                Qblocks[block_idx].sta_idx = pb->sta_idx;
                Qblocks[block_idx].height = pb->height;
                Qblocks[block_idx].ele_num = pb->ele_num;
                Qblocks[block_idx].eles.resize(pb->ele_num);
                Qblocks[block_idx].isP = pb->isP;
                for (int i = 0; i < pb->ele_num; i++)
                {
                    Qblocks[block_idx].eles[i] = data_eles[i];
                }
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
            free(blockBuf);
            free(dataBuf);
            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("recv success time = %lld, recved_head=%d has_processed=%d\n", mksp, recved_head, has_processed );
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
