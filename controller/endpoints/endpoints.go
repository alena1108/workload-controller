package endpoints

import (
	"context"

	"encoding/json"
	"fmt"
	"github.com/rancher/types/apis/core/v1"
	"github.com/rancher/types/config"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"reflect"
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
	// for node port service
	ServiceName string
	// for host port
	PodName string
	//serviceName and podName are mutually exclusive
}

type EndpointsController struct {
	nodeController    v1.NodeController
	serviceController v1.ServiceController
}

type NodesController struct {
	nodes          v1.NodeInterface
	endpointLister v1.EndpointsLister
	serviceLister  v1.ServiceLister
}

type ServicesController struct {
	services       v1.ServiceInterface
	endpointLister v1.EndpointsLister
	serviceLister  v1.ServiceLister
	nodeLister     v1.NodeLister
}

func Register(ctx context.Context, workload *config.WorkloadContext) {
	e := &EndpointsController{
		nodeController:    workload.Core.Nodes("").Controller(),
		serviceController: workload.Core.Services("").Controller(),
	}
	workload.Core.Endpoints("").AddHandler("endpointsController", e.sync)

	n := &NodesController{
		nodes:          workload.Core.Nodes(""),
		endpointLister: workload.Core.Endpoints("").Controller().Lister(),
		serviceLister:  workload.Core.Services("").Controller().Lister(),
	}
	workload.Core.Nodes("").AddHandler("nodesEndpointsController", n.sync)

	s := &ServicesController{
		services:       workload.Core.Services(""),
		endpointLister: workload.Core.Endpoints("").Controller().Lister(),
		serviceLister:  workload.Core.Services("").Controller().Lister(),
		nodeLister:     workload.Core.Nodes("").Controller().Lister(),
	}
	workload.Core.Nodes("").AddHandler("servicesEndpointsController", s.sync)
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
		for nodeName, _ := range nodes {
			e.nodeController.Enqueue("", nodeName)
		}
		// endpoint.name == service.name
		e.serviceController.Enqueue(obj.Namespace, obj.Name)
	}

	return nil
}

func (n *ServicesController) sync(key string, obj *corev1.Service) error {
	if obj == nil || obj.DeletionTimestamp != nil {
		return nil
	}

	ep, err := n.endpointLister.Get(obj.Namespace, obj.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if obj.Kind != "NodePort" {
		return nil
	}
	nodes, err := n.nodeLister.List("".labels.NewSelector())
	if err != nil {
		return err
	}
	newPublicEps, err := convertEndpointToPublicEndpoints(ep, nodes, obj)
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
	toUpdate.Annotations[endpointsAnnotation] = epsToUpdate
	_, err = n.services.Update(toUpdate)

	return nil
}

func (n *NodesController) sync(key string, obj *corev1.Node) error {
	if obj == nil || obj.DeletionTimestamp != nil {
		return nil
	}

	eps, err := n.endpointLister.List("", labels.NewSelector())
	if err != nil {
		return err
	}
	var newPublicEps []PublicEndpoint
	for _, ep := range eps {
		svc, err := n.serviceLister.Get(ep.Namespace, ep.Name)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
		pEps, err := convertEndpointToPublicEndpoints(ep, []*corev1.Node{obj}, svc)
		if err != nil {
			return err
		}
		if len(pEps) == 0 {
			continue
		}
		newPublicEps = append(newPublicEps, pEps...)
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

func convertEndpointToPublicEndpoints(ep *corev1.Endpoints, nodes []*corev1.Node, svc *corev1.Service) ([]PublicEndpoint, error) {
	var eps []PublicEndpoint
	nodeNameToIp := make(map[string]string)

	// figure out on which nodes the service is deployed
	for _, s := range ep.Subsets {
		for _, addr := range s.Addresses {
			if addr.NodeName == nil {
				continue
			}
			nodeName := *addr.NodeName
			if _, ok := nodeNameToIp[nodeName]; ok {
				continue
			}
			for _, node := range nodes {
				if nodeName == node.Name {
					if val, ok := node.Annotations["alpha.kubernetes.io/provided-node-ip"]; ok {
						nodeIp := string(val)
						if nodeIp == "" {
							logrus.Warnf("Node [%s] has no ip address set", nodeName)
						} else {
							nodeNameToIp[nodeName] = nodeIp
						}
						break
					}

				}
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
				ServiceName: ep.Name,
			}
			eps = append(eps, p)
		}
	}

	return eps, nil
}

func (p PublicEndpoint) string() string {
	return fmt.Sprintf("%s_%s_%s_%s_%s_%s", p.NodeName, p.NodeIP, p.Port, p.Protocol, p.ServiceName, p.PodName)
}
