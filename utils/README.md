Sample Test Cases

===========================================================

NOTE: Before testing, clear the Redis cache to ensure determinate behavior

	REDIS-CLI FLUSHALL
NOTE: Before testing, make sure you have 2 transmitter-receivers ready named uw-s1i1 and uw-s1i2

    Reminder: Run flask device-credentials push after every modification to the device-credentials JSON file

R1A -> Receiver 1 is assigned

R2A -> Receiver 2 is assigned

T1A -> Transmitter 1 is assigned

T2A -> Transmitter 2 is assigned

R1C -> Receiver 1 completes

R2C -> Receiver 2 completes

T1C -> Transmitter 1 completes

T2C -> Transmitter 2 completes

A2  -> Add task with priority 2

A5  -> Add task with priority 5

A8  -> Add task with priority 8

D   -> Delete most recent task added

===========================================================

python run-scheduler.py ARGS -> BEHAVIOR
| NO. | ARGS | BEHAVIOR |
| ------ | ------ | ------ |
| 1 | A2 A2 R1A T1A T1C T1A R1C | "No tasks in queue" on second T1A -> waiting on R1C |
| 2 | A2 A2 R1A R1A T1A T1C R1C | "Device in use" on second R1A |
| 3 | A2 A2 R1A T1A T1C T1C R1C | Second T1C does nothing (says "Error" correctly) |
| 4 | A2 A2 R1A T1A T1A T1C R1C | "No tasks in queue" on second T1A ("Device in use" for a transmitter) |
| 5 | A2 A2 R1A T1A R1C R1A T1C | Operates successfully |
| 6 | A2 A2 R1A T1A R1C R1A R1C T1C | Operates successfully |
| 7 | A2 R1A T1A D T1C R1C | T1C and R1C do nothing (says "Error" correctly) |
| 8 | A2 R1A D T1A R1C T1C | "No tasks in queue" on T1A -> only task deleted, R1C does nothing (says "Error" correctly), "Previous assignment failed; do nothing" |
| 9 | A2 T1A R1A T1A T1C R1C | "No tasks in queue" on first T1A |
| 10 | A2 A2 A2 A2 R1A R2A T1A R1C T2A R2C R1A T2C T1C R2A T1A T2A T1C T2C R1C R2C | Operates successfully |
