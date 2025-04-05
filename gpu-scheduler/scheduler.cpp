#include <cstdint>

typedef struct {
    int32_t ID;
    float   GPU_UTILIZATION;
    uint8_t PERIOD; //ms
    uint8_t DEADLINE;
    uint8_t PRIORITY;
} Task;


int execute_task(Task) {
    return 0;
}
