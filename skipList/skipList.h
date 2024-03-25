#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>
#include <string>
#include "../common/definitions.h"
#include "../common/exceptions.h"

namespace skiplist {

	using key_type = def::key_type;
	using value_type = def::value_type;

	class baseDataNode;

	// data and search node defined here
	struct searchDataNode {
		// this node type is only responsible for searching

		// ATTENTION! for the sake of maximum 
		key_type* key;
		size_t layer_number;
		searchDataNode* next = nullptr;
		baseDataNode* parent;
	};

	class baseDataNode {
	private:
		// all values saved in on baseDataNode
		key_type* key;
		value_type value;
		baseDataNode* next = nullptr;

		// save all searchDataNode here
		std::vector<searchDataNode*> nodes;

	public:
		// ATTENTION! the first construction method is only used to construct head-node
		baseDataNode() : key(nullptr) {}
		baseDataNode(const key_type& cur_key) : key(new key_type(cur_key)) {}
		~baseDataNode() {
			// delete all nodes
			for (int i = 0; i < nodes.size(); ++i)
				delete nodes[i];

			// for head-node, key == nullptr
			if (key) delete key;
		}

		baseDataNode*& nextNode() { return this->next; }
		const value_type& getValues() const { return this->value; }
		void addValue(const value_type& new_value) { value = new_value; }

		size_t size() const { return nodes.size(); }
		searchDataNode* getLayer(size_t index) const {
			// 1-base
			assert((index - 1) < nodes.size());
			return nodes[index - 1];
		}

		searchDataNode* addNewSearchNode() {
			searchDataNode* new_node = new searchDataNode();
			new_node->key = this->key; new_node->layer_number = this->nodes.size();
			new_node->parent = this;
			nodes.push_back(new_node);

			// the member 'next' of new_node is set externally
			return new_node;
		}
	};

	class skiplist_type
	{
	private:
		// data members of skiplist here
		double p = 0.5;
		baseDataNode* head = nullptr;

		size_t key_number = 0;

		// method members of skiplist here
		searchDataNode* findAndInsert(const key_type& key,
			const value_type& val, size_t cur_layer, baseDataNode* cur_base_node);
		std::pair<baseDataNode*, size_t> find(const key_type& key,
			size_t cur_layer, baseDataNode* cur_base_node) const;

		// possibility function
		bool eventOccur() const;

	public:
		explicit skiplist_type(double p = 0.5);
		~skiplist_type();

		void put(const key_type& key, const value_type& val);
		std::optional<value_type> get(const key_type& key) const;

		void clear();

		// for hw1 only
		int query_distance(const key_type& key) const;

		size_t size() const;

		class const_iterator
		{
		private:
			baseDataNode* pointer = nullptr;

		public:
			const_iterator(baseDataNode* p = nullptr)
				: pointer(p) {}
			const_iterator& operator++();
			const_iterator operator++(int);
			const value_type& operator*() const;

			const value_type& value() const;
			const key_type& key() const;

			bool operator==(const const_iterator& other) const;
			bool operator!=(const const_iterator& other) const;
		};

		const_iterator cbegin() const;
		const_iterator cend() const;
	};

} // namespace skiplist
