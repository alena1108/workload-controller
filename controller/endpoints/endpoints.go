package endpoints

import (
	"context"

	"encoding/json"
	"fmt"
	"github.com/rancher/types/apis/core/v1"
	"github.com/rancher/types/config"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"reflect"
)

const (
	allEndpoints        = "_all_endpoints_"
	endpointsAnnotation = "field.cattle.io/publicEndpoints"
)

type PublicEndpoint struct {
	NodeName string
	NodeIP   string
	Port     int32
	Protocol string
	// for node port service
	ServiceName string
	// for host port
	PodName string
	//serviceName and podName are mutually exclusive
}

type NodesController struct {
	nodes             v1.NodeInterface
	serviceLister     v1.ServiceLister
	serviceController v1.ServiceController
}

type ServicesController struct {
	services       v1.ServiceInterface
	serviceLister  v1.ServiceLister
	nodeLister     v1.NodeLister
	nodeController v1.NodeController
}

func Register(ctx context.Context, workload *config.WorkloadContext) {
	n := &NodesController{
		nodes:             workload.Core.Nodes(""),
		serviceLister:     workload.Core.Services("").Controller().Lister(),
		serviceController: workload.Core.Services("").Controller(),
	}
	workload.Core.Nodes("").AddHandler("nodesEndpointsController", n.sync)

	s := &ServicesController{
		services:       workload.Core.Services(""),
		serviceLister:  workload.Core.Services("").Controller().Lister(),
		nodeLister:     workload.Core.Nodes("").Controller().Lister(),
		nodeController: workload.Core.Nodes("").Controller(),
	}
	workload.Core.Services("").AddHandler("servicesEndpointsController", s.sync)
}

func (s *ServicesController) sync(key string, obj *corev1.Service) error {
	if obj == nil {
		return nil
	}

	if obj.Spec.Type != "NodePort" {
		return nil
	}

	nodes, err := s.nodeLister.List("", labels.NewSelector())
	if err != nil {
		return err
	}

	if obj.DeletionTimestamp != nil {
		// push changes to all the nodes
		for _, node := range nodes {
			if node.DeletionTimestamp == nil {
				s.nodeController.Enqueue("", node.Name)
			}
		}
		return nil
	}

	// 1. update service with endpoints
	newPublicEps, err := convertServiceToPublicEndpoints(nodes, obj)
	if err != nil {
		return err
	}

	existingPublicEps := getPublicEndpointsFromAnnotations(obj.Annotations)
	if areEqualEndpoints(existingPublicEps, newPublicEps) {
		return nil
	}
	toUpdate := obj.DeepCopy()
	epsToUpdate, err := publicEndpointsToString(newPublicEps)
	if err != nil {
		return err
	}

	logrus.Infof("Updating service [%s] with public endpoints [%v]", key, epsToUpdate)
	if toUpdate.Annotations == nil {
		toUpdate.Annotations = make(map[string]string)
	}
	toUpdate.Annotations[endpointsAnnotation] = epsToUpdate
	_, err = s.services.Update(toUpdate)
	if err != nil {
		return err
	}
	// 2. push changes to all the nodes (only in case service got an update!)
	for _, node := range nodes {
		if node.DeletionTimestamp == nil {
			s.nodeController.Enqueue("", node.Name)
		}
	}

	return nil
}

func (n *NodesController) sync(key string, obj *corev1.Node) error {
	if obj == nil {
		return nil
	}

	svcs, err := n.serviceLister.List("", labels.NewSelector())
	if err != nil {
		return err
	}

	var nodePortSvcs []*corev1.Service
	for _, svc := range svcs {
		if svc.DeletionTimestamp != nil {
			continue
		}
		if svc.Spec.Type == "NodePort" {
			nodePortSvcs = append(nodePortSvcs, svc)
		}
	}

	if obj.DeletionTimestamp != nil {
		// push changes to all services
		for _, svc := range nodePortSvcs {
			n.serviceController.Enqueue(svc.Namespace, svc.Name)
		}
		return nil
	}

	var newPublicEps []PublicEndpoint
	for _, svc := range nodePortSvcs {
		pEps, err := convertServiceToPublicEndpoints([]*corev1.Node{obj}, svc)
		if err != nil {
			return err
		}
		if len(pEps) == 0 {
			continue
		}
		newPublicEps = append(newPublicEps, pEps...)
	}

	// 1. update node with endpoints
	existingPublicEps := getPublicEndpointsFromAnnotations(obj.Annotations)
	if areEqualEndpoints(existingPublicEps, newPublicEps) {
		return nil
	}
	toUpdate := obj.DeepCopy()
	epsToUpdate, err := publicEndpointsToString(newPublicEps)
	if err != nil {
		return err
	}
	logrus.Infof("Updating node [%s] with public endpoints [%v]", key, epsToUpdate)
	if toUpdate.Annotations == nil {
		toUpdate.Annotations = make(map[string]string)
	}
	toUpdate.Annotations[endpointsAnnotation] = epsToUpdate
	_, err = n.nodes.Update(toUpdate)

	// 2. push changes to the svcs (only when node info changed)
	for _, svc := range nodePortSvcs {
		n.serviceController.Enqueue(svc.Namespace, svc.Name)
	}

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

func getPublicEndpointsFromAnnotations(annotations map[string]string) []PublicEndpoint {
	var eps []PublicEndpoint
	if annotations == nil {
		return eps
	}
	if val, ok := annotations[endpointsAnnotation]; ok {
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

func convertServiceToPublicEndpoints(nodes []*corev1.Node, svc *corev1.Service) ([]PublicEndpoint, error) {
	var eps []PublicEndpoint
	nodeNameToIp := make(map[string]string)

	for _, node := range nodes {
		if val, ok := node.Annotations["alpha.kubernetes.io/provided-node-ip"]; ok {
			nodeIp := string(val)
			if nodeIp == "" {
				logrus.Warnf("Node [%s] has no ip address set", node.Name)
			} else {
				nodeNameToIp[node.Name] = nodeIp
			}
		}
	}

	for nodeName, nodeIp := range nodeNameToIp {
		for _, port := range svc.Spec.Ports {
			if port.NodePort == 0 {
				continue
			}
			p := PublicEndpoint{
				NodeName:    nodeName,
				NodeIP:      nodeIp,
				Port:        port.NodePort,
				Protocol:    string(port.Protocol),
				ServiceName: svc.Name,
			}
			eps = append(eps, p)
		}
	}

	return eps, nil
}

func (p PublicEndpoint) string() string {
	return fmt.Sprintf("%s_%s_%s_%s_%s_%s", p.NodeName, p.NodeIP, p.Port, p.Protocol, p.ServiceName, p.PodName)
}
