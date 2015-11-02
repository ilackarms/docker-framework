/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scheduler

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	"strconv"
	"github.com/gogo/protobuf/proto"
	util "github.com/mesos/mesos-go/mesosutil"
	"fmt"
	"strings"
)

type ExampleScheduler struct {
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	cpuPerTask    float64
	memPerTask    float64
	DockerImage   string
	DockerPorts   []*mesos.ContainerInfo_DockerInfo_PortMapping
	DockerCommand   string
}

func NewExampleScheduler(dockerImage string, dockerPorts string, command string, taskCount int, cpuPerTask float64, memPerTask float64) *ExampleScheduler {
	portmappingstrings := strings.Split(dockerPorts,",")
	fmt.Printf("Processing mappings:" + dockerPorts + "\n%v", portmappingstrings)
	var ports []*mesos.ContainerInfo_DockerInfo_PortMapping
	if len(portmappingstrings) > 0 {
		for _, mapping := range portmappingstrings {
			if mapping != "" {
				fmt.Printf("Processing mapping:" + mapping)
				hostPort, err := strconv.Atoi(strings.Split(mapping, ":")[0])
				if err != nil {
					fmt.Errorf("Error parsing docker ports\nportmappingstrings: %v\n" + err.Error() + "\nsize: %v", portmappingstrings, len(portmappingstrings))
				}
				containerPort, err := strconv.Atoi(strings.Split(mapping, ":")[1])
				if err != nil {
					fmt.Errorf("Error parsing docker ports\n" + err.Error())
				}
				hp := uint32(hostPort)
				cp := uint32(containerPort)
				ports = append(ports,
					&mesos.ContainerInfo_DockerInfo_PortMapping{
						HostPort: &hp,
						ContainerPort: &cp,
					})
			}
		}
	}

	return &ExampleScheduler{
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    taskCount,
		cpuPerTask:    cpuPerTask,
		memPerTask:    memPerTask,
		DockerImage:   dockerImage,
		DockerPorts:   ports,
		DockerCommand: command,
	}
}

func (sched *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	fmt.Println("Scheduler Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	fmt.Println("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
	fmt.Println("Scheduler Disconnected")
}

func (sched *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)
	for _, offer := range offers {
		remainingCpus := getOfferCpu(offer)
		remainingMems := getOfferMem(offer)

		var tasks []*mesos.TaskInfo
		for sched.cpuPerTask <= remainingCpus &&
		sched.memPerTask <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			dockerInfo := &mesos.ContainerInfo_DockerInfo{
				Image: &sched.DockerImage,
				PortMappings: sched.DockerPorts,
			}

			containerType := mesos.ContainerInfo_DOCKER

			containerInfo := &mesos.ContainerInfo{
				Type: &containerType,
				Docker: dockerInfo,
			}

			commandInfo := &mesos.CommandInfo{
				Value: &sched.DockerCommand,
			}

			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", sched.cpuPerTask),
					util.NewScalarResource("mem", sched.memPerTask),
				},
				Container: containerInfo,
				Command: commandInfo,
			}
			fmt.Printf("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= sched.cpuPerTask
			remainingMems -= sched.memPerTask
		}
		fmt.Println("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}

}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	fmt.Println("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (sched *ExampleScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	fmt.Printf("Offer '%v' rescinded.\n", *id)
}

func (sched *ExampleScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	fmt.Printf("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *ExampleScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	fmt.Printf("Slave '%v' lost.\n", *id)
}

func (sched *ExampleScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	fmt.Printf("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (sched *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
	fmt.Println("Scheduler received error:", err)
}
