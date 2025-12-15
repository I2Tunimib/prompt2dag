#!/usr/bin/env python3
"""
DAG Graph Traversal Validator (CLI) - Multi-Dimensional Scoring
Validates the logical correctness of generated pipeline JSON structures.
Uses objective, multi-dimensional scoring (0-10) without arbitrary penalty weights.

Dimensions:
1. Structural Integrity (25%) - Schema completeness
2. Node Connectivity (25%) - Reachability from entry points
3. Component Usage (25%) - % of components actually used
4. Graph Validity (25%) - Acyclicity, terminals, dangling refs
5. Task-Component Consistency (25%) - Mapping correctness

Exit codes:
- 0: PASS
- 1: FAIL
- 2: WARNING (if --fail-on-warning)
"""

import json
import argparse
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import os
import sys
import re

# ---------------------------
# Validation structures
# ---------------------------

class ValidationStatus(Enum):
    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"


@dataclass
class ValidationIssue:
    severity: ValidationStatus
    category: str
    description: str
    affected_nodes: List[str] = field(default_factory=list)
    details: Dict = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "severity": self.severity.value,
            "category": self.category,
            "description": self.description,
            "affected_nodes": self.affected_nodes,
            "details": self.details
        }


@dataclass
class DimensionScore:
    """Represents a single quality dimension score."""
    dimension: str
    score: float  # 0.0 to 100.0
    max_score: float  # Always 100.0
    criteria_passed: int
    criteria_total: int
    details: Dict = field(default_factory=dict)
    
    @property
    def percentage(self) -> float:
        return self.score
    
    def to_dict(self):
        return {
            "dimension": self.dimension,
            "score": round(self.score, 2),
            "percentage": f"{self.score:.1f}%",
            "criteria_passed": self.criteria_passed,
            "criteria_total": self.criteria_total,
            "details": self.details
        }


@dataclass
class TraversalReport:
    timestamp: str
    validation_status: ValidationStatus
    overall_score: float  # 0.0 to 10.0
    dimension_scores: List[DimensionScore]
    issues: List[ValidationIssue]
    graph_statistics: Dict
    connectivity_analysis: Dict
    recommendations: List[str]
    
    def to_dict(self):
        return {
            "validation_metadata": {
                "timestamp": self.timestamp,
                "status": self.validation_status.value,
                "overall_score": round(self.overall_score, 2),
                "scoring_method": "multi_dimensional_objective"
            },
            "dimension_scores": [ds.to_dict() for ds in self.dimension_scores],
            "issues": [issue.to_dict() for issue in self.issues],
            "graph_statistics": self.graph_statistics,
            "connectivity_analysis": self.connectivity_analysis,
            "recommendations": self.recommendations
        }
    
    def print_report(self):
        status_icon = {
            ValidationStatus.PASS: "✅",
            ValidationStatus.WARNING: "⚠️",
            ValidationStatus.FAIL: "❌"
        }
        
        print("\n" + "="*80)
        print("DAG GRAPH TRAVERSAL VALIDATION REPORT")
        print("="*80)
        print(f"\nTimestamp: {self.timestamp}")
        print(f"Overall Status: {status_icon[self.validation_status]} {self.validation_status.value}")
        print(f"Overall Score: {self.overall_score:.2f}/10.00")
        
        # Dimension Scores
        print("\n" + "-"*80)
        print("QUALITY DIMENSIONS")
        print("-"*80)
        for ds in self.dimension_scores:
            bar_length = 40
            filled = int((ds.score / 100.0) * bar_length)
            bar = "█" * filled + "░" * (bar_length - filled)
            print(f"\n{ds.dimension:.<30} {ds.score:5.1f}% [{bar}]")
            print(f"  Criteria: {ds.criteria_passed}/{ds.criteria_total} passed")
            if ds.details:
                for key, value in ds.details.items():
                    if isinstance(value, bool):
                        symbol = "✓" if value else "✗"
                        print(f"  {symbol} {key}: {value}")
                    elif isinstance(value, list):
                        if len(value) <= 3:
                            print(f"  • {key}: {value}")
                        else:
                            print(f"  • {key}: {len(value)} items")
                    else:
                        print(f"  • {key}: {value}")
        
        # Issues Summary
        fail_count = sum(1 for i in self.issues if i.severity == ValidationStatus.FAIL)
        warning_count = sum(1 for i in self.issues if i.severity == ValidationStatus.WARNING)
        
        print("\n" + "-"*80)
        print("ISSUES SUMMARY")
        print("-"*80)
        print(f"Critical Failures: {fail_count}")
        print(f"Warnings: {warning_count}")
        print(f"Total Issues: {len(self.issues)}")
        
        if self.issues:
            print("\nDETAILED ISSUES:")
            for idx, issue in enumerate(self.issues, 1):
                icon = status_icon[issue.severity]
                print(f"\n{idx}. {icon} {issue.severity.value} - {issue.category}")
                print(f"   Description: {issue.description}")
                if issue.affected_nodes:
                    if len(issue.affected_nodes) <= 5:
                        print(f"   Affected Nodes: {', '.join(issue.affected_nodes)}")
                    else:
                        print(f"   Affected Nodes: {', '.join(issue.affected_nodes[:5])} ... and {len(issue.affected_nodes) - 5} more")
                if issue.details:
                    for key, value in issue.details.items():
                        if isinstance(value, list) and len(value) > 5:
                            print(f"   {key}: {value[:5]} ... (+{len(value)-5} more)")
                        else:
                            print(f"   {key}: {value}")
        
        # Graph Statistics
        print("\n" + "-"*80)
        print("GRAPH STATISTICS")
        print("-"*80)
        for key, value in self.graph_statistics.items():
            print(f"{key}: {value}")
        
        # Connectivity Analysis
        print("\n" + "-"*80)
        print("CONNECTIVITY ANALYSIS")
        print("-"*80)
        for key, value in self.connectivity_analysis.items():
            if isinstance(value, list):
                if len(value) <= 10:
                    print(f"{key}: {', '.join(str(v) for v in value) if value else 'None'}")
                else:
                    print(f"{key}: {len(value)} items - {', '.join(str(v) for v in value[:5])} ... (+{len(value)-5} more)")
            else:
                print(f"{key}: {value}")
        
        # Recommendations
        if self.recommendations:
            print("\n" + "-"*80)
            print("RECOMMENDATIONS")
            print("-"*80)
            for idx, rec in enumerate(self.recommendations, 1):
                print(f"{idx}. {rec}")
        
        print("\n" + "="*80 + "\n")


# ---------------------------
# Validator
# ---------------------------

class DAGGraphValidator:
    """
    Validates the logical correctness of pipeline DAG structures.
    Uses multi-dimensional objective scoring.
    """
    
    def __init__(self, pipeline_json: Dict):
        self.pipeline_json = pipeline_json
        self.timestamp = datetime.now().isoformat()
        self.issues: List[ValidationIssue] = []

        # Components
        raw_components = pipeline_json.get("components") or []
        self.components: Dict[str, Dict] = {}
        for c in raw_components:
            if isinstance(c, dict) and "id" in c:
                comp = dict(c)
                comp.setdefault("type", comp.get("category", "Unknown"))
                self.components[c["id"]] = comp

        # Flow structure
        fs = pipeline_json.get("flow_structure") \
             or pipeline_json.get("detailed_flow_structure") \
             or {}
        self.flow_structure = self._normalize_flow_structure(fs)

        # Derived
        self.nodes: Dict[str, Dict] = self.flow_structure.get("nodes", {})
        self.entry_points: List[str] = self.flow_structure.get("entry_points", []) or []
        self.edges: List[Dict] = self.flow_structure.get("edges", []) or []
        self.parallel_blocks: Dict = self.flow_structure.get("parallel_blocks", {}) or {}

        # Ensure adjacency exists
        self._ensure_adjacency()

    # ---------- Public API ----------
    def validate(self) -> TraversalReport:
        """Run comprehensive DAG validation and return a TraversalReport."""
        
        # Basic structure validation
        self._validate_basic_structure()

        # Entry points validation
        self._validate_entry_points_present()

        # Connectivity analysis
        connectivity_analysis = self._validate_connectivity()

        # Acyclicity validation
        self._validate_acyclic()

        # Exit points validation
        self._validate_exit_points()

        # Component-node consistency
        self._validate_component_consistency()

        # Node reachability
        self._validate_node_reachability(connectivity_analysis.get("reachable_nodes", []))

        # Compute statistics
        graph_stats = self._compute_graph_statistics(connectivity_analysis)

        # Compute multi-dimensional scores
        status, score, dimension_scores = self._compute_overall_status()

        # Generate recommendations
        recommendations = self._generate_recommendations(connectivity_analysis)

        return TraversalReport(
            timestamp=self.timestamp,
            validation_status=status,
            overall_score=score,
            dimension_scores=dimension_scores,
            issues=self.issues,
            graph_statistics=graph_stats,
            connectivity_analysis=connectivity_analysis,
            recommendations=recommendations
        )

    # ---------- Normalization helpers ----------
    @staticmethod
    def _normalize_flow_structure(fs: Dict) -> Dict:
        if not isinstance(fs, dict):
            return {"nodes": {}, "entry_points": [], "edges": []}
        fs.setdefault("nodes", {})
        fs.setdefault("entry_points", [])
        fs.setdefault("edges", [])
        return fs

    def _ensure_adjacency(self):
        """Ensure each node has next_nodes and merge edges."""
        
        # Ensure next_nodes exists
        for nid, nd in self.nodes.items():
            if not isinstance(nd, dict):
                self.nodes[nid] = {}
                nd = self.nodes[nid]
            nd.setdefault("next_nodes", [])
            nd.setdefault("kind", nd.get("type", "Task"))
            if "type" not in nd and "kind" in nd:
                nd["type"] = nd["kind"]

        # Merge edges into next_nodes
        for e in self.edges:
            if not isinstance(e, dict):
                continue
            src, dst = e.get("from"), e.get("to")
            if src and dst and src in self.nodes:
                nxt = self.nodes[src].setdefault("next_nodes", [])
                if dst not in nxt:
                    nxt.append(dst)

        # Infer entry points if missing
        if not self.entry_points:
            indegree = {nid: 0 for nid in self.nodes.keys()}
            for nid, nd in self.nodes.items():
                for nxt in nd.get("next_nodes", []):
                    if nxt in indegree:
                        indegree[nxt] += 1
            inferred = [nid for nid, deg in indegree.items() if deg == 0]
            if inferred:
                self.entry_points = inferred
                self.issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    category="Entry Points",
                    description="No entry_points provided; inferred candidates by zero indegree",
                    affected_nodes=inferred,
                    details={"inferred_entry_points": inferred}
                ))

    # ---------- Validations ----------
    def _validate_basic_structure(self):
        if not self.components:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.FAIL,
                category="Structure",
                description="No components found in JSON"
            ))
        if not self.flow_structure:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.FAIL,
                category="Structure",
                description="No flow structure found (flow_structure or detailed_flow_structure missing)"
            ))
        if not self.nodes:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.FAIL,
                category="Structure",
                description="No nodes defined in flow structure"
            ))

    def _validate_entry_points_present(self):
        for entry in self.entry_points:
            if entry not in self.nodes:
                self.issues.append(ValidationIssue(
                    severity=ValidationStatus.FAIL,
                    category="Entry Points",
                    description=f"Entry point '{entry}' not found in nodes",
                    affected_nodes=[entry]
                ))

    def _validate_connectivity(self) -> Dict:
        """Validate node-level connectivity with DFS traversal."""
        
        if not self.entry_points:
            return {
                "reachable_nodes": [],
                "unreachable_nodes": list(self.nodes.keys()),
                "reachable_components": [],
                "unreachable_components": list(self.components.keys()),
                "traversal_path": [],
                "traversal_order_valid": True
            }

        visited: Set[str] = set()
        traversal_path: List[str] = []
        stack: List[str] = list(self.entry_points)

        all_node_ids = set(self.nodes.keys())
        referenced_nodes = set(self.entry_points)

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            if current not in self.nodes:
                self.issues.append(ValidationIssue(
                    severity=ValidationStatus.FAIL,
                    category="Connectivity",
                    description=f"Node '{current}' is referenced but not defined in nodes block",
                    affected_nodes=[current]
                ))
                continue

            visited.add(current)
            traversal_path.append(current)

            next_nodes = self.nodes[current].get("next_nodes", []) or []
            for nxt in reversed(next_nodes):
                referenced_nodes.add(nxt)
                if nxt not in all_node_ids:
                    self.issues.append(ValidationIssue(
                        severity=ValidationStatus.FAIL,
                        category="Connectivity",
                        description=f"Dangling reference from '{current}' to undefined node '{nxt}'",
                        affected_nodes=[current, nxt]
                    ))
                if nxt not in visited:
                    stack.append(nxt)

        # Node reachability
        reachable_nodes = list(visited)
        unreachable_nodes = [nid for nid in self.nodes.keys() if nid not in visited]

        # Component reachability
        reachable_components: Set[str] = set()
        for nid in visited:
            nd = self.nodes.get(nid, {})
            kind = nd.get("kind", nd.get("type", "Task"))
            ctid = nd.get("component_type_id")
            if kind == "Task":
                if ctid and ctid in self.components:
                    reachable_components.add(ctid)
                elif (not ctid) and (nid in self.components):
                    reachable_components.add(nid)
                elif ctid and ctid not in self.components:
                    self.issues.append(ValidationIssue(
                        severity=ValidationStatus.FAIL,
                        category="Consistency",
                        description=f"Task node '{nid}' references unknown component_type_id '{ctid}'",
                        affected_nodes=[nid],
                        details={"component_type_id": ctid}
                    ))
        
        unreachable_components = [cid for cid in self.components.keys() if cid not in reachable_components]

        if unreachable_nodes:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.FAIL,
                category="Connectivity",
                description="Unreachable nodes detected from entry points",
                affected_nodes=unreachable_nodes,
                details={"count": len(unreachable_nodes)}
            ))

        if unreachable_components:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.WARNING,
                category="Consistency",
                description="Components defined but unused (no Task node references them)",
                affected_nodes=unreachable_components,
                details={"count": len(unreachable_components)}
            ))

        return {
            "reachable_nodes": reachable_nodes,
            "unreachable_nodes": unreachable_nodes,
            "reachable_components": sorted(list(reachable_components)),
            "unreachable_components": unreachable_components,
            "traversal_path": traversal_path,
            "traversal_order_valid": len(traversal_path) == len(set(traversal_path))
        }

    def _validate_acyclic(self):
        """Check for cycles using DFS."""
        visited = set()
        rec_stack = set()

        def dfs(node: str, path: List[str]) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for nxt in self.nodes.get(node, {}).get("next_nodes", []):
                if nxt not in visited:
                    cyc = dfs(nxt, path.copy())
                    if cyc:
                        return cyc
                elif nxt in rec_stack:
                    cycle_start = path.index(nxt) if nxt in path else 0
                    return path[cycle_start:] + [nxt]
            rec_stack.remove(node)
            return None

        for entry in self.entry_points:
            if entry not in visited and entry in self.nodes:
                cyc = dfs(entry, [])
                if cyc:
                    self.issues.append(ValidationIssue(
                        severity=ValidationStatus.FAIL,
                        category="Acyclicity",
                        description="Cycle detected in DAG - workflow must be acyclic",
                        affected_nodes=cyc,
                        details={"cycle_path": " -> ".join(cyc)}
                    ))
                    return

    def _validate_exit_points(self):
        """Check terminal nodes exist."""
        terminal_nodes = [nid for nid, nd in self.nodes.items() if not nd.get("next_nodes")]
        
        if len(terminal_nodes) == 0:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.WARNING,
                category="Exit Points",
                description="No terminal nodes found - potential cycle or missing terminal"
            ))
        elif len(terminal_nodes) > 1:
            self.issues.append(ValidationIssue(
                severity=ValidationStatus.WARNING,
                category="Exit Points",
                description="Multiple terminal nodes detected",
                affected_nodes=terminal_nodes,
                details={"terminal_node_count": len(terminal_nodes)}
            ))

    def _validate_component_consistency(self):
        """Validate Task node to component mapping."""
        for nid, nd in self.nodes.items():
            kind = nd.get("kind", nd.get("type", "Task"))
            ctid = nd.get("component_type_id")

            if kind == "Task":
                if ctid:
                    if ctid not in self.components:
                        self.issues.append(ValidationIssue(
                            severity=ValidationStatus.FAIL,
                            category="Consistency",
                            description=f"Task node '{nid}' references unknown component_type_id '{ctid}'",
                            affected_nodes=[nid],
                            details={"component_type_id": ctid}
                        ))
                else:
                    if nid not in self.components:
                        self.issues.append(ValidationIssue(
                            severity=ValidationStatus.WARNING,
                            category="Consistency",
                            description=f"Task node '{nid}' missing component_type_id and node id is not a component id",
                            affected_nodes=[nid]
                        ))
            else:
                if ctid:
                    self.issues.append(ValidationIssue(
                        severity=ValidationStatus.WARNING,
                        category="Consistency",
                        description=f"Non-Task node '{nid}' has component_type_id '{ctid}' (usually null)",
                        affected_nodes=[nid],
                        details={"kind": kind, "component_type_id": ctid}
                    ))

    def _validate_node_reachability(self, reachable_nodes: List[str]):
        """Placeholder for extensibility."""
        pass

    # ---------- Statistics ----------
    def _compute_graph_statistics(self, connectivity: Dict) -> Dict:
        """Compute graph statistics."""
        node_count = len(self.nodes)
        edge_count = sum(len(nd.get("next_nodes", [])) for nd in self.nodes.values())

        max_depth = self._calculate_max_depth()

        kind_distribution = {}
        for nid, nd in self.nodes.items():
            kind = nd.get("kind", nd.get("type", "Task"))
            kind_distribution[kind] = kind_distribution.get(kind, 0) + 1

        comp_category_dist = {}
        for comp in self.components.values():
            cat = comp.get("category") or comp.get("type", "Unknown")
            comp_category_dist[cat] = comp_category_dist.get(cat, 0) + 1

        terminal_nodes = [nid for nid, nd in self.nodes.items() if not nd.get("next_nodes")]

        return {
            "total_components": len(self.components),
            "total_nodes_in_flow": node_count,
            "total_edges": edge_count,
            "entry_point_count": len(self.entry_points),
            "terminal_node_count": len(terminal_nodes),
            "max_pipeline_depth": max_depth,
            "avg_branching_factor": (edge_count / node_count) if node_count > 0 else 0.0,
            "node_kind_distribution": kind_distribution,
            "component_category_distribution": comp_category_dist,
            "has_parallel_blocks": bool(self.parallel_blocks),
            "reachable_nodes_count": len(connectivity.get("reachable_nodes", [])),
            "unreachable_nodes_count": len(connectivity.get("unreachable_nodes", []))
        }

    def _calculate_max_depth(self) -> int:
        """Calculate maximum pipeline depth."""
        if not self.entry_points:
            return 0

        def dfs_depth(nid: str, visited: Set[str]) -> int:
            if nid in visited or nid not in self.nodes:
                return 0
            visited.add(nid)
            nxts = self.nodes[nid].get("next_nodes", [])
            if not nxts:
                return 1
            return 1 + max(dfs_depth(n, visited.copy()) for n in nxts)

        return max(dfs_depth(ep, set()) for ep in self.entry_points if ep in self.nodes) or 0

    # ---------- Multi-Dimensional Scoring ----------
    def _compute_overall_status(self) -> Tuple[ValidationStatus, float, List[DimensionScore]]:
        """
        Compute overall status and score using multi-dimensional objective scoring.
        Each dimension is scored 0-100%, then averaged and scaled to 10.
        """
        dimension_scores = []
        
        # Dimension 1: Structural Integrity
        struct_score, struct_criteria = self._score_structural_integrity()
        dimension_scores.append(DimensionScore(
            dimension="Structural Integrity",
            score=struct_score,
            max_score=100.0,
            criteria_passed=struct_criteria['passed'],
            criteria_total=struct_criteria['total'],
            details=struct_criteria['details']
        ))
        
        # Dimension 2: Node Connectivity
        conn_score, conn_criteria = self._score_node_connectivity()
        dimension_scores.append(DimensionScore(
            dimension="Node Connectivity",
            score=conn_score,
            max_score=100.0,
            criteria_passed=conn_criteria['passed'],
            criteria_total=conn_criteria['total'],
            details=conn_criteria['details']
        ))
        
        # Dimension 3: Component Usage
        comp_score, comp_criteria = self._score_component_usage()
        dimension_scores.append(DimensionScore(
            dimension="Component Usage",
            score=comp_score,
            max_score=100.0,
            criteria_passed=comp_criteria['passed'],
            criteria_total=comp_criteria['total'],
            details=comp_criteria['details']
        ))
        
        # Dimension 4: Graph Validity
        valid_score, valid_criteria = self._score_graph_validity()
        dimension_scores.append(DimensionScore(
            dimension="Graph Validity",
            score=valid_score,
            max_score=100.0,
            criteria_passed=valid_criteria['passed'],
            criteria_total=valid_criteria['total'],
            details=valid_criteria['details']
        ))
        
        # Dimension 5: Task-Component Consistency
        consist_score, consist_criteria = self._score_consistency()
        dimension_scores.append(DimensionScore(
            dimension="Task-Component Consistency",
            score=consist_score,
            max_score=100.0,
            criteria_passed=consist_criteria['passed'],
            criteria_total=consist_criteria['total'],
            details=consist_criteria['details']
        ))
        
        # Overall score: Average of all dimensions, scaled to 10
        avg_percentage = sum(ds.score for ds in dimension_scores) / len(dimension_scores)
        overall_score = (avg_percentage / 100.0) * 10.0
        
        # Determine status
        fail_count = sum(1 for i in self.issues if i.severity == ValidationStatus.FAIL)
        warning_count = sum(1 for i in self.issues if i.severity == ValidationStatus.WARNING)
        
        if fail_count > 0 or overall_score < 5.0:
            status = ValidationStatus.FAIL
        elif warning_count > 0 or overall_score < 8.0:
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.PASS
        
        return status, overall_score, dimension_scores

    def _score_structural_integrity(self) -> Tuple[float, Dict]:
        """
        Score: 0-100% based on basic schema completeness.
        Criteria:
        - Has components (25%)
        - Has flow_structure (25%)
        - Has nodes (25%)
        - Has entry_points (25%)
        """
        criteria = []
        details = {}
        
        has_components = len(self.components) > 0
        criteria.append(has_components)
        details['has_components'] = has_components
        
        has_flow = bool(self.flow_structure)
        criteria.append(has_flow)
        details['has_flow_structure'] = has_flow
        
        has_nodes = len(self.nodes) > 0
        criteria.append(has_nodes)
        details['has_nodes'] = has_nodes
        
        has_entry = len(self.entry_points) > 0
        criteria.append(has_entry)
        details['has_entry_points'] = has_entry
        
        passed = sum(criteria)
        total = len(criteria)
        score = (passed / total) * 100.0
        
        return score, {'passed': passed, 'total': total, 'details': details}

    def _score_node_connectivity(self) -> Tuple[float, Dict]:
        """
        Score: 0-100% based on node reachability.
        Formula: (reachable_nodes / total_nodes) * 100
        """
        total_nodes = len(self.nodes)
        if total_nodes == 0:
            return 0.0, {'passed': 0, 'total': 1, 'details': {'reason': 'No nodes defined'}}
        
        reachable = self._count_reachable_nodes()
        score = (reachable / total_nodes) * 100.0
        
        details = {
            'reachable_nodes': reachable,
            'total_nodes': total_nodes,
            'unreachable_nodes': total_nodes - reachable
        }
        
        passed = 1 if reachable == total_nodes else 0
        
        return score, {'passed': passed, 'total': 1, 'details': details}

    def _count_reachable_nodes(self) -> int:
        """Helper to count reachable nodes via DFS."""
        if not self.entry_points:
            return 0
        
        visited = set()
        stack = list(self.entry_points)
        
        while stack:
            current = stack.pop()
            if current in visited or current not in self.nodes:
                continue
            visited.add(current)
            for nxt in self.nodes[current].get("next_nodes", []):
                if nxt not in visited:
                    stack.append(nxt)
        
        return len(visited)

    def _score_component_usage(self) -> Tuple[float, Dict]:
        """
        Score: 0-100% based on component usage.
        Formula: (used_components / total_components) * 100
        """
        total_components = len(self.components)
        if total_components == 0:
            return 100.0, {'passed': 1, 'total': 1, 'details': {'reason': 'No components defined (N/A)'}}
        
        used_components = set()
        for nid, nd in self.nodes.items():
            kind = nd.get("kind", nd.get("type", "Task"))
            ctid = nd.get("component_type_id")
            if kind == "Task":
                if ctid and ctid in self.components:
                    used_components.add(ctid)
                elif (not ctid) and (nid in self.components):
                    used_components.add(nid)
        
        used_count = len(used_components)
        score = (used_count / total_components) * 100.0
        
        details = {
            'used_components': used_count,
            'total_components': total_components,
            'unused_components': total_components - used_count
        }
        
        passed = 1 if used_count == total_components else 0
        
        return score, {'passed': passed, 'total': 1, 'details': details}

    def _score_graph_validity(self) -> Tuple[float, Dict]:
        """
        Score: 0-100% based on graph validity checks.
        Weighted criteria:
        - Is acyclic (40%)
        - Has terminal nodes (30%)
        - No dangling references (30%)
        """
        criteria = []
        details = {}
        
        # Check acyclicity
        is_acyclic = not any(i.category == "Acyclicity" for i in self.issues)
        criteria.append((is_acyclic, 40))
        details['is_acyclic'] = is_acyclic
        
        # Check terminal nodes exist
        terminal_nodes = [nid for nid, nd in self.nodes.items() if not nd.get("next_nodes")]
        has_terminal = len(terminal_nodes) > 0
        criteria.append((has_terminal, 30))
        details['has_terminal_nodes'] = has_terminal
        details['terminal_node_count'] = len(terminal_nodes)
        
        # Check no dangling references
        has_dangling = any("Dangling reference" in i.description for i in self.issues)
        no_dangling = not has_dangling
        criteria.append((no_dangling, 30))
        details['no_dangling_references'] = no_dangling
        
        # Weighted score
        score = sum(weight for passed, weight in criteria if passed)
        passed_count = sum(1 for passed, _ in criteria if passed)
        total_count = len(criteria)
        
        return score, {'passed': passed_count, 'total': total_count, 'details': details}

    def _score_consistency(self) -> Tuple[float, Dict]:
        """
        Score: 0-100% based on Task node consistency.
        Formula: (valid_task_nodes / total_task_nodes) * 100
        """
        task_nodes = [
            (nid, nd) for nid, nd in self.nodes.items()
            if nd.get("kind", nd.get("type", "Task")) == "Task"
        ]
        
        if not task_nodes:
            return 100.0, {'passed': 1, 'total': 1, 'details': {'reason': 'No Task nodes (N/A)'}}
        
        valid_count = 0
        for nid, nd in task_nodes:
            ctid = nd.get("component_type_id")
            if ctid:
                if ctid in self.components:
                    valid_count += 1
            else:
                if nid in self.components:
                    valid_count += 1
        
        total_task_nodes = len(task_nodes)
        score = (valid_count / total_task_nodes) * 100.0
        
        details = {
            'valid_task_nodes': valid_count,
            'total_task_nodes': total_task_nodes,
            'invalid_task_nodes': total_task_nodes - valid_count
        }
        
        passed = 1 if valid_count == total_task_nodes else 0
        
        return score, {'passed': passed, 'total': 1, 'details': details}

    # ---------- Recommendations ----------
    def _generate_recommendations(self, connectivity: Dict) -> List[str]:
        """Generate actionable recommendations based on issues."""
        recs = []
        
        if connectivity.get("unreachable_components"):
            recs.append(
                "Connect unused components: Ensure each component is referenced by at least one Task node "
                "(via component_type_id) reachable from entry points."
            )
        
        if any(i.category == "Connectivity" and "Dangling reference" in i.description for i in self.issues):
            recs.append(
                "Fix dangling node references: Every id in next_nodes must be a key under flow nodes."
            )
        
        if any(i.category == "Consistency" and "unknown component_type_id" in i.description for i in self.issues):
            recs.append(
                "Correct component_type_id values on Task nodes: They must match component ids listed in 'components'."
            )
        
        if any(i.category == "Acyclicity" for i in self.issues):
            recs.append(
                "Remove cycles: DAGs must be acyclic; remove or rewire the edge(s) reported in the cycle path."
            )
        
        if any(i.category == "Entry Points" for i in self.issues):
            recs.append(
                "Define explicit 'entry_points' under the flow structure; they must reference node ids present in 'nodes'."
            )
        
        if not self.issues:
            recs.append(
                "Graph structure is valid. Consider adding extra metadata (upstream policies, retry configs) for clarity."
            )
        
        return recs


# ---------------------------
# CLI helpers
# ---------------------------

def read_json(path: str) -> Dict:
    """Read JSON file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, data: Dict):
    """Write JSON file."""
    out_dir = os.path.dirname(path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(
        description="DAG Graph Traversal Validator (CLI) - Multi-Dimensional Objective Scoring",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Path to pipeline JSON file (Step 1 output)")
    parser.add_argument("--output-json", default=None, help="Path to save validation JSON report")
    parser.add_argument("--no-print", action="store_true", help="Do not print human-readable report to stdout")
    parser.add_argument("--fail-on-warning", action="store_true", help="Exit with non-zero code on WARNING")
    args = parser.parse_args()

    try:
        pipeline_json = read_json(args.input)
    except Exception as e:
        print(f"ERROR: Failed to read JSON: {e}", file=sys.stderr)
        sys.exit(2)

    validator = DAGGraphValidator(pipeline_json)
    report = validator.validate()

    if not args.no_print:
        report.print_report()

    # Save JSON
    out_path = args.output_json
    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.splitext(os.path.basename(args.input))[0]
        out_path = f"dag_validation_report_{base}_{ts}.json"
    
    try:
        write_json(out_path, report.to_dict())
        if not args.no_print:
            print(f"Validation report saved to {out_path}")
    except Exception as e:
        print(f"ERROR: Failed to save validation JSON: {e}", file=sys.stderr)

    # Exit codes
    if report.validation_status == ValidationStatus.FAIL:
        sys.exit(1)
    if args.fail_on_warning and report.validation_status == ValidationStatus.WARNING:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()