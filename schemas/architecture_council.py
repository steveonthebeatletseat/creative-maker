"""Schemas for the architecture-council multi-agent workflow."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class CouncilRequirement(BaseModel):
    requirement_id: str = Field(description="Stable ID like REQ-001")
    title: str
    description: str
    downstream_dependency: str
    source_anchor: str
    priority: Literal["critical", "high", "medium"]


class QualityDimension(BaseModel):
    name: str
    why_it_matters: str
    measurement: str
    pass_threshold: str


class RequirementsBrief(BaseModel):
    objective_statement: str
    scope_boundaries: list[str] = Field(min_length=1)
    non_negotiables: list[CouncilRequirement] = Field(min_length=6)
    quality_dimensions: list[QualityDimension] = Field(min_length=5)
    known_unknowns: list[str] = Field(min_length=1)


class AgentNode(BaseModel):
    agent_name: str
    purpose: str
    input_contract: list[str] = Field(min_length=1)
    output_contract: list[str] = Field(min_length=1)
    pass_fail_gate: str


class StrategyOption(BaseModel):
    option_id: str = Field(description="Stable ID like option_a")
    name: str
    summary: str
    design_principles: list[str] = Field(min_length=3)
    agent_graph: list[AgentNode] = Field(min_length=4)
    qa_gates: list[str] = Field(min_length=4)
    strengths: list[str] = Field(min_length=3)
    risks: list[str] = Field(min_length=3)
    best_for: str


class StrategyPortfolio(BaseModel):
    options: list[StrategyOption] = Field(min_length=3, max_length=3)


class RiskFinding(BaseModel):
    severity: Literal["critical", "high", "medium"]
    failure_mode: str
    why_likely: str
    downstream_damage: str
    mitigation: str
    validation_test: str


class RedTeamReport(BaseModel):
    option_id: str
    overall_risk_level: Literal["critical", "high", "medium", "low"]
    findings: list[RiskFinding] = Field(min_length=5)
    kill_criteria: list[str] = Field(min_length=1)


class OptionScore(BaseModel):
    option_id: str
    quality_fidelity: int = Field(ge=1, le=10)
    traceability: int = Field(ge=1, le=10)
    robustness: int = Field(ge=1, le=10)
    downstream_fit: int = Field(ge=1, le=10)
    operational_clarity: int = Field(ge=1, le=10)
    weighted_total: float = Field(ge=0, le=10)
    justification: str


class DecisionReport(BaseModel):
    winner_option_id: str
    winner_name: str
    confidence: float = Field(ge=0, le=1)
    scorecard: list[OptionScore] = Field(min_length=3, max_length=3)
    reasons_winner_beats_runner_up: list[str] = Field(min_length=2)
    reopen_triggers: list[str] = Field(min_length=2)


class WorkflowStage(BaseModel):
    stage_name: str
    objective: str
    owner_agent: str
    required_inputs: list[str] = Field(min_length=1)
    output_artifact: str
    gate_to_pass: str


class BlueprintReport(BaseModel):
    mission: str
    recommended_strategy_id: str
    recommended_strategy_name: str
    workflow: list[WorkflowStage] = Field(min_length=5)
    operator_checklist: list[str] = Field(min_length=5)
    quality_dashboard: list[str] = Field(min_length=4)
    first_round_deliverables: list[str] = Field(min_length=4)


class ArchitectureCouncilRun(BaseModel):
    source_path: str
    generated_at: str
    requirements_brief: RequirementsBrief
    strategy_portfolio: StrategyPortfolio
    red_team_reports: list[RedTeamReport] = Field(min_length=3)
    decision_report: DecisionReport
    blueprint_report: BlueprintReport
