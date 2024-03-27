#include "skipList.h"

namespace skiplist {

    skiplist_type::skiplist_type(double p) : p(p) {
        head = new baseDataNode();
        head->addNewSearchNode();
    }

    skiplist_type::~skiplist_type() {
        while (head->nextNode()) {
            baseDataNode* next_node = head->nextNode();
            head->nextNode() = next_node->nextNode();
            delete next_node;
        }
        delete head;
    }

    bool skiplist_type::eventOccur() const {
        return static_cast<double>(std::rand()) / RAND_MAX <= p;
    }

    searchDataNode* skiplist_type::findAndInsert(const key_type& key,
        const value_type& val, size_t cur_layer, baseDataNode* cur_base_node) {

        // continue search
        if (cur_layer) {
            // find next node
            searchDataNode* cur_search_node = cur_base_node->getLayer(cur_layer);
            while (cur_search_node->next && *(cur_search_node->next->key) <= key) {
                cur_search_node = cur_search_node->next;
            }

            // assign new index
            cur_base_node = cur_search_node->parent;

            // find the corresponding key
            if (cur_search_node->key && *(cur_search_node->key) == key) {
                cur_base_node->addValue(val);
                return nullptr;
            }
            // the key of next node is larger than 'key' or the next node is nullptr
            else {
                searchDataNode* next_node = findAndInsert(key, val, cur_layer - 1, cur_base_node);

                // new node created
                if (next_node) {
                    next_node->next = cur_search_node->next;
                    cur_search_node->next = next_node;

                    // push up with a certain possibility
                    if (!eventOccur()) next_node = nullptr;
                    else next_node = next_node->parent->addNewSearchNode();
                }

                return next_node;
            }
        }
        // already the first layer and the key hasn't been found
        else {
            baseDataNode* new_base_node = new baseDataNode(key);
            new_base_node->nextNode() = cur_base_node->nextNode();
            cur_base_node->nextNode() = new_base_node;
            new_base_node->addValue(val);
            ++key_number;
            return new_base_node->addNewSearchNode();
        }
    }

    std::pair<baseDataNode*, size_t> skiplist_type::find(const key_type& key,
        size_t cur_layer, baseDataNode* cur_base_node) const {
        size_t new_steps = 0;

        // continue search
        if (cur_layer) {
            // find next node
            searchDataNode* cur_search_node = cur_base_node->getLayer(cur_layer);
            while (cur_search_node->next && *(cur_search_node->next->key) <= key) {
                cur_search_node = cur_search_node->next;
                ++new_steps;
            }

            // assign new index
            cur_base_node = cur_search_node->parent;

            // find the corresponding key
            if (cur_search_node->key && *(cur_search_node->key) == key) {
                return std::make_pair(cur_base_node, new_steps);
            }
            // the key of next node is larger than 'key' or the next node is nullptr
            else {
                std::pair<baseDataNode*, size_t> find_pair = find(key, cur_layer - 1, cur_base_node);
                return std::make_pair(find_pair.first, find_pair.second + new_steps + 1);
            }
        }
        // already the first layer and the key hasn't been found
        else {
            return std::make_pair(cur_base_node, new_steps);
        }
    }

    void skiplist_type::put(const key_type& key, const value_type& val) {
        searchDataNode* new_node = findAndInsert(key, val, head->size(), head);

        // a new layer will be added
        if (new_node) {
            searchDataNode* new_head_node = head->addNewSearchNode();
            new_node->next = new_head_node->next;
            new_head_node->next = new_node;
        }
    }

    std::optional<value_type> skiplist_type::get(const key_type& key) const {
        baseDataNode* find_base_node = find(key, head->size(), head).first;
        searchDataNode* first_search_data_node = find_base_node->getLayer(1);

        // if find the right node
        if (first_search_data_node->key && *(first_search_data_node->key) == key) {
            return find_base_node->getValues();
        }

        // return the default value of value_type
        return std::nullopt;
    }

    void skiplist_type::clear() {
        while (head->nextNode()) {
            baseDataNode* next_node = head->nextNode();
            head->nextNode() = next_node->nextNode();
            delete next_node;
        }
        delete head;
        key_number = 0;

        head = new baseDataNode();
        head->addNewSearchNode();
    }

    int skiplist_type::query_distance(const key_type& key) const {
        return find(key, head->size(), head).second + 1;
    }

    size_t skiplist_type::size() const {
        return key_number;
    }

    skiplist_type::const_iterator& skiplist_type::const_iterator::operator++() {
        if (!pointer) {
            throw exception::iterator_null();
        }

        pointer = pointer->nextNode();
        return *this;
    }

    skiplist_type::const_iterator skiplist_type::const_iterator::operator++(int) {
        if (!pointer) {
            throw exception::iterator_null();
        }

        skiplist_type::const_iterator old_it = *this;
        pointer = pointer->nextNode();
        return old_it;
    }

    const value_type& skiplist_type::const_iterator::operator*() const {
        return this->pointer->getValues();
    }

    const value_type& skiplist_type::const_iterator::value() const {
        return this->pointer->getValues();
    }

	const key_type& skiplist_type::const_iterator::key() const {
        return *(this->pointer->getLayer(1)->key);
    }

    skiplist_type::const_iterator skiplist_type::cbegin() const {
        return const_iterator(head->nextNode());
    }

    skiplist_type::const_iterator skiplist_type::cend() const {
        return const_iterator();
    }

    bool skiplist_type::const_iterator::operator==(const const_iterator& other) const {
        return this->pointer == other.pointer;
    }

	bool skiplist_type::const_iterator::operator!=(const const_iterator& other) const {
        return this->pointer != other.pointer;
    }

} // namespace skiplist
