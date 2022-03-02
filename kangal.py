from neo4j import GraphDatabase
from datetime import datetime
import argparse

CAP_TREE = {}


def reset_db(tx):
    tx.run("match (n) set n.attached = False;")
    tx.run("match (n:Tier) detach delete n;")


def get_highvalue_nodes(tx):
    result = tx.run("MATCH (n) WHERE n.highvalue=True RETURN n.objectid AS node")
    return [record["node"] for record in result]


def create_tier0_group(tx, nodes):
    tx.run("CREATE (m:Tier {name:'Tier0', objectid:'Tier0', type:'cap', members:%s, level:0, index:0})" % nodes)


def get_tier_group_members(tx, name):
    result = tx.run("MATCH (t:Tier {name:'%s'}) RETURN t.members as members" % name)
    return result.single()[0]


def is_member_attached(tx, objectid):
    result = tx.run("MATCH (n {objectid:'%s'}) RETURN n.attached as attached" % objectid)
    return result.single()[0]


def is_member_highvalue(tx, objectid):
    result = tx.run("MATCH (n {objectid:'%s'}) RETURN n.highvalue as highvalue" % objectid)
    return result.single()[0]


def get_node_incoming_relations(tx, objectid):
    result = tx.run("MATCH (n {objectid: '%s'})<-[rel]-(m) RETURN DISTINCT type(rel) as relation" % objectid)
    return [record["relation"] for record in result]


def get_connected_nodes(tx, objectid, relation):
    result = tx.run(
        "MATCH (n {objectid:'%s'})<-[rel:%s]-(m) WHERE m.highvalue = False and m <> n RETURN m.objectid AS objectid" % (
            objectid, relation))
    return [record["objectid"] for record in result]


def check_tier_membership(tx, parent_tier, relation):
    result = tx.run(
        "MATCH (pt:Tier {name:'%s'})<-[rel:%s]-(ct:Tier) RETURN ct.name as name" % (parent_tier, "cap_" + relation))

    status = result.single()
    if status is not None:
        return status[0]
    else:
        return None


def create_tier_group(tx, parent_tier, relation, level, index, nodes):
    name = "Group" + str(index)
    tx.run("CREATE (m:Tier {name:'%s', objectid:'%s', type:'cap', members:%s, level:%d, index:%d})" % (
        name, name, nodes, level, index))
    tx.run("MATCH (pt:Tier {name:'%s'}),(ct:Tier {name:'%s'}) CREATE (ct)-[rel:%s {type:'cap'}]->(pt)" % (
        parent_tier, name, "cap_" + relation))
    return name


def add_member_to_tier_group(tx, group, objectid):
    tx.run("MATCH (t:Tier {name:'%s'}) SET t.members = t.members + '%s'" % (group, objectid))


def attach_member(tx, objectid):
    tx.run("MATCH (n {objectid:'%s'}) SET n.attached = True" % objectid)


def get_non_attached_member_count(tx):
    result = tx.run("MATCH (n) WHERE n.attached = False RETURN count(n) as cnt")
    return result.single()[0]


def create_tier_groups(tx, parent_tiers, level, index):
    print(datetime.now())
    level += 1
    groups = set()

    processed_relation_stats = 0
    created_group_stats = 0
    processed_group_stats = 0
    processed_node_stats = 0

    processed_group_stats += len(parent_tiers)

    for parent_tier in parent_tiers:
        members = get_tier_group_members(tx, parent_tier)
        processed_node_stats += len(members)

        for member in members:

            if is_member_attached(tx, member):
                continue

            relations = get_node_incoming_relations(tx, member)
            processed_relation_stats += len(relations)

            for relation in relations:
                connected_nodes = get_connected_nodes(tx, member, relation)
                if len(connected_nodes) > 0:
                    group = check_tier_membership(tx, parent_tier, relation)

                    if group is None:
                        index += 1
                        new_group = create_tier_group(tx, parent_tier, relation, level, index, connected_nodes)
                        groups.add(new_group)
                        created_group_stats += 1
                    else:
                        if not is_member_highvalue(tx, member):
                            add_member_to_tier_group(tx, group, member)

            attach_member(tx, member)

    print("[+] Level %d STATS" % level)
    print("[+] Processed Group Count    %d" % processed_group_stats)
    print("[+] Created Group Count      %d" % created_group_stats)
    print("[+] Processed Node Count     %d" % processed_node_stats)
    print("[+] Processed Rel Count      %d" % processed_relation_stats)

    if get_non_attached_member_count(tx) == 0:
        return

    if processed_group_stats + processed_node_stats + processed_relation_stats == 0:
        return

    create_tier_groups(tx, groups, level, index)


def get_tier_group_childs(tx, name):
    childs = []
    result = tx.run("MATCH (n:Tier {name:'%s'})<-[]-(m:Tier) RETURN m.name as name, m.members as members" % name)
    for record in result:
        childs.append({
            "name": record["name"],
            "members": record["members"],
            "member_count": len(record["members"]),
            "sum_child_count": 0,
            "sum_member_count": 0,
            "childs": []
        })
    return childs


def load_cap_tree(tx, node):
    for child in node["childs"]:
        child["childs"] = get_tier_group_childs(tx, child["name"])
        child["child_count"] = len(child["childs"])
        load_cap_tree(tx, child)


def calculate_cap_tree_scores(node):
    for child in node["childs"]:
        sum_member_count, member_count, sum_child_count, child_count = calculate_cap_tree_scores(child)
        child["sum_member_count"] += member_count
        child["sum_child_count"] += child_count

        node["sum_member_count"] += sum_member_count + member_count
        node["sum_child_count"] += sum_child_count + child_count

    return node["sum_member_count"], node["member_count"], node["sum_child_count"], node["child_count"]


def update_cap_tree(tx, node):
    for child in node["childs"]:
        update_cap_tree(tx, child)

    tx.run(
        "MATCH (n:Tier {name:'%s'}) SET n.sum_member_count = %s, n.sum_child_count = %s, n.member_count = %s, n.child_count = %s" % (
            node["name"], node["sum_member_count"], node["sum_child_count"], node["member_count"], node["child_count"]))


def calculate_tier_group_scores(tx):
    global CAP_TREE

    initial_tier_group = "Tier0"
    members = get_tier_group_members(tx, initial_tier_group)

    CAP_TREE["name"] = initial_tier_group
    CAP_TREE["members"] = members
    CAP_TREE["member_count"] = len(members)
    CAP_TREE["childs"] = get_tier_group_childs(tx, initial_tier_group)
    CAP_TREE["child_count"] = len(CAP_TREE["childs"])
    CAP_TREE["sum_child_count"] = 0
    CAP_TREE["sum_member_count"] = 0

    load_cap_tree(tx, CAP_TREE)
    _, member_count, _, child_count = calculate_cap_tree_scores(CAP_TREE)
    CAP_TREE["sum_member_count"] += member_count
    CAP_TREE["sum_child_count"] += child_count

    update_cap_tree(tx, CAP_TREE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combined Attack Paths for BloodHound")
    parser.add_argument("--username", help="Neo4j database username", required=True)
    parser.add_argument("--password", help="Neo4j database password", required=True)
    parser.add_argument("--host", help="Neo4j database host", default="localhost")
    parser.add_argument("--port", help="Neo4j database bolt port", default="7687")
    args = parser.parse_args()

    uri = "bolt://%s:%s" % (args.host, args.port)
    driver = GraphDatabase.driver(uri, auth=(args.username, args.password))
    tx = driver.session()

    print("[+] Reset database")
    reset_db(tx)

    tier0_nodes = get_highvalue_nodes(tx)
    print("[+] Tier0 node count: %d" % len(tier0_nodes))

    # creating tier groups
    print("[+] Creating tier groups")
    create_tier0_group(tx, tier0_nodes)
    create_tier_groups(tx, ["Tier0"], 0, 0)

    # calculate cap tree scores
    print("[+] Calculating combined attack path scores")
    calculate_tier_group_scores(tx)

    driver.close()
