package session

// MoveBinding rebinds a key from its current node to another node. It returns
// the previous node id, or "" when the key had no binding.
func (s *Store) MoveBinding(key, toNodeID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.binds[key]
	s.binds[key] = toNodeID
	return prev
}

// OnNodeEvicted handles node eviction.
func (s *Store) OnNodeEvicted(nodeID string) int {
	return 0
}
