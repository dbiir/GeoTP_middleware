package org.apache.shardingsphere.infra.statistics.monitor;

import java.util.List;

class AVLTree {
    // 获取节点高度
    int height(AVLTreeNode N) {
        if (N == null)
            return 0;
        return N.getHeight();
    }

    // 右旋转操作
    AVLTreeNode rightRotate(AVLTreeNode y) {
        AVLTreeNode x = y.getLeft();
        AVLTreeNode T2 = x.getRight();

        // rotate
        y.setLeft(T2);
        x.setRight(y);

        // update the height
        y.setHeight(Math.max(height(y.left), height(y.right)) + 1);
        x.setHeight(Math.max(height(x.left), height(x.right)) + 1);

        // return new root
        return x;
    }

    // Left Rotate
    AVLTreeNode leftRotate(AVLTreeNode x) {
        AVLTreeNode y = x.getRight();
        AVLTreeNode T2 = y.getLeft();

        // Right Rotate
        y.setLeft(x);
        x.setRight(T2);

        // update the height
        x.setHeight(Math.max(height(x.left), height(x.right)) + 1);
        y.setHeight(Math.max(height(y.left), height(y.right)) + 1);

        // return new root
        return y;
    }

    // fetch the balance factor
    int getBalance(AVLTreeNode N) {
        if (N == null)
            return 0;
        return height(N.left) - height(N.right);
    }

    // insert operation
    AVLTreeNode insert(AVLTreeNode node, LockMetaData lock) {
        // 1. BST
        if (node == null)
            return new AVLTreeNode(lock);

        if (lock.getKey() < node.getKey())
            node.setLeft(insert(node.left, lock));
        else if (lock.getKey() > node.getKey())
            node.setRight(insert(node.right, lock));
        else // Duplicate keys are not allowed
            return node;

        // 2. update the height of node
        node.setHeight(1 + Math.max(height(node.left), height(node.right)));

        // 3. check the balance
        int balance = getBalance(node);

        // Rotate if unbalanced
        // Left Left Case
        if (balance > 1 && lock.getKey() < node.getLeft().getKey())
            return rightRotate(node);

        // Right Right Case
        if (balance < -1 && lock.getKey() > node.getRight().getKey())
            return leftRotate(node);

        // Left Right Case
        if (balance > 1 && lock.getKey() > node.getLeft().getKey()) {
            node.setLeft(leftRotate(node.getLeft()));
            return rightRotate(node);
        }

        // Right Left Case
        if (balance < -1 && lock.getKey() < node.getRight().getKey()) {
            node.setRight(rightRotate(node.getRight()));
            return leftRotate(node);
        }

        return node;
    }

    // 最小值节点
    AVLTreeNode minValueNode(AVLTreeNode node) {
        AVLTreeNode current = node;
        while (current.getLeft() != null)
            current = current.getLeft();
        return current;
    }

    // 删除节点操作
    AVLTreeNode deleteNode(AVLTreeNode root, LockMetaData lock) {
        if (root == null)
            return root;

        if (lock.getKey() < root.getKey())
            root.setLeft(deleteNode(root.getLeft(), lock));
        else if (lock.getKey() > root.getKey())
            root.setRight(deleteNode(root.getRight(), lock));
        else {
            // have a child node or no child node
            if ((root.getLeft() == null) || (root.getRight() == null)) {
                root = root.getLeft() != null ? root.getLeft() : root.getRight();
            } else {
                // have two child nodes, find the minimum node in the right subtree
                AVLTreeNode temp = minValueNode(root.getRight());
                root.setLockMetaData(temp.getLockMetaData());
                root.setRight(deleteNode(root.getRight(), temp.getLockMetaData()));
            }
        }

        if (root == null)
            return root;

        // 2. update the height of node
        root.setHeight(Math.max(height(root.getLeft()), height(root.getRight())) + 1);

        // 3. check balance
        int balance = getBalance(root);

        // LL
        if (balance > 1 && getBalance(root.getLeft()) >= 0)
            return rightRotate(root);

        // LR
        if (balance > 1 && getBalance(root.getLeft()) < 0) {
            root.setLeft(leftRotate(root.getLeft()));
            return rightRotate(root);
        }

        // RR
        if (balance < -1 && getBalance(root.getRight()) <= 0)
            return leftRotate(root);

        // RL
        if (balance < -1 && getBalance(root.getRight()) > 0) {
            root.setRight(rightRotate(root.getRight()));
            return leftRotate(root);
        }

        return root;
    }

    // get the lock metadata with the key
    public LockMetaData search(AVLTreeNode root, int key) {
        if (root == null)
            return null;

        if (key < root.getKey())
            return search(root.getLeft(), key);   // find in the left subtree
        else if (key > root.getKey())
            return search(root.getRight(), key);  // find in the right subtree
        else
            return root.getLockMetaData();  // find the lock metadata
    }

    // scan the node with the key range [low, high]
    public void rangeQuery(AVLTreeNode root, int low, int high, List<LockMetaData> lockMetaDataList) {
        if (root == null)
            return;

        // if current node's value is greater than the lower bound, then visit the left subtree
        if (root.getKey() > low)
            rangeQuery(root.getLeft(), low, high, lockMetaDataList);

        // if current node's value is within the range, then add the metadata into the list
        if (root.getKey() >= low && root.getKey() <= high) {
            lockMetaDataList.add(root.getLockMetaData());
        }

        // if current node's value is less than the upper bound, then visit the right subtree
        if (root.getKey() < high)
            rangeQuery(root.getRight(), low, high, lockMetaDataList);
    }

    // mid-order traversal
    void verboseOrder(AVLTreeNode node) {
        if (node != null) {
            System.out.print(node.getKey() + " ");
            verboseOrder(node.left);
            verboseOrder(node.right);
        }
    }
}
