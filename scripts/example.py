import logging

from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
from typing import List, Dict, Union

logger = logging.getLogger(__name__)


class Neo4jUtil:
    """Neo4j操作类"""

    def __init__(self, conf: dict):
        # 连接数据库
        self.graph = GraphDatabase.driver(
            uri='neo4j://{}:{}'.format(conf['host'], conf['port']),
            auth=(conf['username'], conf['password']))

    def run(self, cypher: str) -> List:
        """执行自定义语句"""
        logger.info('[Neo4jClient run] cypher = {}'.format(cypher))
        with self.graph.session() as session:
            results = session.run(cypher)
            return [result for result in results]

    def labels2cypher(self, labels: List[str]):
        """标签列表转cypher语句"""
        return ':'.join(labels)

    def properties2cypher(self, properties: Dict[str, Union[str, int, bool, float, list]]):
        """属性字典转cypher语句"""
        items = []
        for key, value in properties.items():
            if value is None:
                items.append('SET n.`{}` = ""'.format(key))
            elif isinstance(value, str):
                items.append('SET n.`{}` = "{}"'.format(key, value))
            elif isinstance(value, (int, float, bool, list)):
                items.append('SET n.`{}` = {}'.format(key, value))
            else:
                logger.warning('[Neo4jClient properties2cypher] unsupported type <{}>'.format(value))
        return ' '.join(items)

    def set_unique(self, labels: List[str], property_name: str):
        """节点唯一性约束"""
        try:
            cypher = 'CREATE CONSTRAINT ON (n: {}) ASSERT n.`{}` IS UNIQUE'.format(
                self.labels2cypher(labels), property_name)
            self.run(cypher)
            return {'status': True, 'message': 'succeed'}
        except Exception:
            return {'status': True, 'message': 'exists'}

    def exists_node(self, labels: List[str], name: str):
        """节点是否存在"""
        cypher = 'MATCH (n:{}) WHERE n.name="{}" RETURN ID(n)'.format(self.labels2cypher(labels), name)
        results = self.run(cypher)
        return True if results else False

    def serialize_node(self, node: Node):
        """序列化节点"""
        node_ = {
            'id': node.id,
            'labels': list(node.labels),
            'properties': node._properties
        }
        return node_

    def create_node(self, labels: List[str], name: str, properties: dict = {}):
        """新增节点"""
        data = {'status': True, 'message': 'succeed', 'node': []}
        if self.exists_node(labels, name):
            logger.info('[Neo4jClient create_node] node <{}> <{}>'.format(labels, name))
            data['status'] = False
            data['message'] = 'exists'
            return data
        cypher = 'CREATE (n: {}) SET n.name="{}" {} RETURN n'.format(
            self.labels2cypher(labels), name, self.properties2cypher(properties))
        results = self.run(cypher)
        data['node'] = self.serialize_node(results[0][0])
        return data

    def delete_node(self, id_: int):
        """删除节点"""
        try:
            cypher = 'MATCH (n) WHERE ID(n) = {} DELETE n'.format(id_)
            self.run(cypher)
            return {'status': True, 'message': 'deleted'}
        except ConstraintError:
            return {'status': False, 'message': 'still has relations'}

    def get_node(self, id_: int):
        """获取节点"""
        cypher = 'MATCH (n) WHERE ID(n) = {} RETURN n'.format(id_)
        results = self.run(cypher)
        if results:
            return {'status': True, 'message': 'succeed', 'node': self.serialize_node(results[0][0])}
        return {'status': False, 'message': 'not exists'}

    def update_node(self, id_: int, properties: dict):
        """更新节点"""
        cypher = 'MATCH (n) WHERE ID(n) = {} {} RETURN n'.format(id_, self.properties2cypher(properties))
        results = self.run(cypher)
        if results:
            return {'status': True, 'message': 'updated', 'node': self.serialize_node(results[0][0])}
        return {'status': False, 'message': 'not exists'}

    def delete_property(self, id_: int, property: str):
        """删除属性key"""
        cypher = 'MATCH (n) WHERE ID(n) = {} REMOVE n.`{}`'.format(id_, property)
        self.run(cypher)

    def serialize_relation(self, relation: Relationship):
        """序列化关系"""
        relation = {
            'id': relation.id,
            'type': relation.type,
            'head_id': relation.start_node.id,
            'tail_id': relation.end_node.id,
            'properties': relation._properties
        }
        return relation

    def get_relation(self, id_: int):
        """获取关系"""
        if id_ is None:
            raise ValueError('relation id not found')
        elif not isinstance(id_, int):
            raise ValueError('id not is int.')
        cypher = f"MATCH ()-[r]->() WHERE ID(r)={id_} RETURN r"
        results = self.run(cypher)
        if results:
            return {'status': True, 'message': 'succeed', 'relation': self.serialize_relation(results[0][0])}
        return {'status': False, 'message': 'not exists'}

    def create_relation(self, head_id: int, tail_id: int, properties: dict, rtype: str):
        """创建关系"""
        data = {'status': True, 'message': 'succeed', 'data': []}
        if self.exists_relation(head_id, tail_id, rtype):
            logger.info('[Neo4jClient create_relation][{}]->(r:{})-[{}]'.format(head_id, rtype, tail_id))
            data['status'] = False
            data['message'] = 'exists'
            return data
        cypher = "match (n1), (n2) where id(n1)={} and id(n2)={} create (n1)-[n:`{}`]->(n2) {} return n".format(
            head_id, tail_id, rtype, self.properties2cypher(properties))
        results = self.run(cypher)
        data['relation'] = self.serialize_relation(results[0][0])
        return data

    def update_relation(self, _id: int, properties: dict):
        """更新关系"""
        cypher = 'MATCH ()-[n]-() WHERE ID(n) = {} {} return n'.format(_id, self.properties2cypher(properties))
        results = self.run(cypher)
        if results:
            return {'status': True, 'message': 'updated', 'node': self.serialize_relation(results[0][0])}
        return {'status': False, 'message': 'relation not exists'}

    def exists_relation(self, head_id: int, tail_id: int, rtype: str, strict: bool = False):
        """判断关系是否存在"""
        cypher = 'MATCH (n)-[r]->(m)' if strict else 'MATCH (n)-[r]-(m)'
        cypher = '{} WHERE ID(n)={} AND ID(m)={} '.format(cypher, head_id, tail_id)
        cypher += "AND type(r) = '{}' RETURN r".format(rtype)
        results = self.run(cypher)
        return True if results else False

    def delete_relation(self, id_: int):
        """删除关系"""
        cypher = 'MATCH ()-[r]-() WHERE ID(r) = {} DELETE r'.format(id_)
        self.run(cypher)
        return {'status': True, 'message': 'deleted'}
