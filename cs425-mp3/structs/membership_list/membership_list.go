package ml

import (
	"log"
	"sort"
)

type MembershipList struct {
	Members map[string]int
	Version int32
	curr_id int
}

func CreateMembershipList(addresses []string, version ...int32) MembershipList {
	id := 0
	mList := make(map[string]int)
	for _, member := range addresses {
		mList[member] = id
		id++
	}
	return MembershipList{Members: mList, Version: version[0], curr_id: id}
}

func (m *MembershipList) Add(address string) {
	if _, exists := m.Members[address]; exists {
		log.Printf("[WARNING] %v already exists in the MembershipList", address)
		return
	}
	m.Members[address] = m.curr_id
	m.Version++
	m.curr_id++
}

func (m *MembershipList) Delete(address string) {
	if _, exists := m.Members[address]; exists {
		delete(m.Members, address)
		m.Version++
		return
	}
	log.Printf("[WARNING] %v does not exist in the MembershipList", address)
}

func (m *MembershipList) GetMembers() []string {
	out := make([]string, len(m.Members))
	idx := 0
	for key := range m.Members {
		out[idx] = key
		idx++
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func (m *MembershipList) GetVersion() int32 {
	return m.Version
}

func (m *MembershipList) Length() int {
	return len(m.Members)
}
