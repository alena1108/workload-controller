package endpoints

import (
	"context"

	"github.com/rancher/types/apis/core/v1"
	"github.com/rancher/types/config"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"encoding/json"
	"reflect"
	"fmt"
)

const (
	allNodes            = "_all_nodes_"
	endpointsAnnotation = "field.cattle.io/publicEndpoints"
)

type PublicEndpoint struct {
	NodeName string
	NodeIP   string
	Port     int32
	Protocol string
	// for node ip
	ServiceName string
	// for host port
	PodName string
	//serviceName and podName are mutually exclusive
}

type EndpointsController struct {
	nodeController v1.NodeController
	serviceController v1.ServiceController
}

type NodesController struct {
	nodes          v1.NodeInterface
	endpointLister v1.EndpointsLister
}

func Register(ctx context.Context, workload *config.WorkloadContext) {
	e := &EndpointsController{
		nodeController: workload.Core.Nodes("").Controller(),
		serviceController: workload.Core.Services("").Controller(),
	}
	workload.Core.Endpoints("").AddHandler("endpointsController", e.sync)

	n := &NodesController{
		nodes:          workload.Core.Nodes(""),
		endpointLister: workload.Core.Endpoints("").Controller().Lister(),
	}
	workload.Core.Nodes("").AddHandler("nodesEndpointsController", n.sync)
}

func (e *EndpointsController) sync(key string, obj *corev1.Endpoints) error {
	if obj == nil {
		// schedule upate for all nodes
		e.nodeController.Enqueue("", allNodes)
	} else {
		nodes := getNodesToUpdate(obj)
		if len(nodes) == 0 {
			return nil
		}
		for nodeName, _ := range nodes{
			e.nodeController.Enqueue("", nodeName)
		}
		// endpoint.name == service.name
		e.serviceController.Enqueue(obj.Namespace, obj.Name)
	}

	return nil
}

func (n *NodesController) sync(key string, obj *corev1.Node) error {
	if obj == nil {
		return nil
	}

	eps, err := n.endpointLister.List("", labels.NewSelector())
	if err != nil {
		return err
	}
	var newPublicEps []PublicEndpoint
	for _, ep := range eps {
		pEps := convertEndpointToNodePublicEndpoint(ep, obj)
		if len(pEps) == 0 {
			continue
		}
		newPublicEps = append(newPublicEps, pEps...)
	}

	existingPublicEps := getPublicEndpointsFromNode(obj)
	if areEqualEndpoints(existingPublicEps, newPublicEps) {
		return nil
	}
	toUpdate := obj.DeepCopy()
	epsToUpdate, err := publicEndpointsToString(newPublicEps)
	if err != nil {
		return err
	}
	logrus.Infof("Updating node [%s] with public endpoints [%v]", key, epsToUpdate)
	toUpdate.Annotations[endpointsAnnotation] = epsToUpdate
	_, err = n.nodes.Update(toUpdate)

	return err
}

func areEqualEndpoints(one []PublicEndpoint, two []PublicEndpoint) bool {
	oneMap := make(map[string]bool)
	twoMap := make(map[string]bool)
	for _, value := range one {
		oneMap[value.string()] = true
	}
	for _, value := range two {
		twoMap[value.string()] = true
	}
	return reflect.DeepEqual(oneMap, twoMap)
}

func publicEndpointsToString(eps []PublicEndpoint) (string, error) {
	b, err := json.Marshal(eps)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getPublicEndpointsFromNode(node *corev1.Node) []PublicEndpoint {
	var eps []PublicEndpoint
	if node.Annotations == nil {
		return eps
	}
	if val, ok := node.Annotations[endpointsAnnotation]; ok {
		err := json.Unmarshal([]byte(val), &eps)
		if err != nil {
			logrus.Errorf("Failed to read public endpoints from annotation %v", err)
			return eps
		}
	}
	return eps
}

func getNodesToUpdate(ep *corev1.Endpoints) map[string]bool {
	nodeNames := make(map[string]bool)
	for _, s := range ep.Subsets {
		for _, addr := range s.Addresses {
			if addr.NodeName == nil {
				continue
			}
			nodeNames[*addr.NodeName] = true
		}
	}
	return nodeNames
}

func convertEndpointToNodePublicEndpoint(ep *corev1.Endpoints, node *corev1.Node) []PublicEndpoint {
	var eps []PublicEndpoint
	nodeName := node.Name
	nodeIp := ""
	if val, ok := node.Annotations["alpha.kubernetes.io/provided-node-ip"]; ok {
		nodeIp = string(val)
	}
	if nodeIp == "" {
		logrus.Warnf("Node [%s] has no ip address set", nodeName)
		return eps
	}

	for _, s := range ep.Subsets {
		found := false
		for _, addr := range s.Addresses {
			if addr.NodeName == nil {
				continue
			}
			if *addr.NodeName == nodeName {
				found = true
				break
			}
		}
		if found {
			for _, port := range s.Ports {
				p := PublicEndpoint{
					NodeName:    node.Name,
					NodeIP:      nodeIp,
					Port:        port.Port,
					Protocol:    string(port.Protocol),
					ServiceName: ep.Name,
				}
				eps = append(eps, p)
			}
		}
	}

	return eps
}


func (p PublicEndpoint)string() string {
	return fmt.Sprintf("%s_%s_%s_%s_%s_%s", p.NodeName, p.NodeIP, p.Port, p.Protocol, p.ServiceName, p.PodName)
}