#include <cstdint>

//
// Created by MacBook Pro on 2025-03-11.
//
struct Task {
    int32_t I32_ID;
    float   GPU_UTILIZATION;
    float   CPU_UTILIZATION;
    uint8_t PERIOD; //ms
    uint8_t PRIORITY;
};

