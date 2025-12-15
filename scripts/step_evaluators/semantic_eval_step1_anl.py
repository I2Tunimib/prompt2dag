#!/usr/bin/env python3
"""
Semantic Evaluation for ANALYSIS Tasks (Step 1)
- Focuses on RECALL (coverage) over precision
- Appropriate length assessment for analysis
- BERTScore scaling calibrated for expansion tasks
"""

import json
import argparse
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
import os
import re
import sys

@dataclass
class EvaluationResult:
    metric_name: str
    precision: float
    recall: float
    f1: float
    normalized_score: float  # 0-10 scaled
    raw_score: float  # Keep original for transparency
    details: Dict = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "metric": self.metric_name,
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "raw_score": round(self.raw_score, 4),
            "normalized_score": round(self.normalized_score, 2),
            "details": self.details
        }

@dataclass
class ComprehensiveReport:
    timestamp: str
    task_type: str  # NEW: "analysis", "summarization", "paraphrase"
    original_text_length: int
    generated_text_length: int
    bertscore_result: Optional[EvaluationResult]
    rouge_result: EvaluationResult
    granular_analysis: Dict
    summary: Dict
    
    def to_dict(self):
        return {
            "evaluation_metadata": {
                "timestamp": self.timestamp,
                "task_type": self.task_type,
                "original_text_length": self.original_text_length,
                "generated_text_length": self.generated_text_length
            },
            "metrics": {
                "bertscore": self.bertscore_result.to_dict() if self.bertscore_result else None,
                "rouge1": self.rouge_result.to_dict()
            },
            "granular_analysis": self.granular_analysis,
            "summary": self.summary
        }
    
    def print_report(self):
        print("\n" + "="*80)
        print(f"SEMANTIC EVALUATION REPORT - {self.task_type.upper()} TASK")
        print("="*80)
        print(f"\nTimestamp: {self.timestamp}")
        print(f"Original Text Length: {self.original_text_length} characters")
        print(f"Generated Text Length: {self.generated_text_length} characters")
        
        # BERTScore
        if self.bertscore_result:
            print("\n" + "-"*80)
            print("BERTSCORE (Semantic Similarity)")
            print("-"*80)
            print(f"Precision: {self.bertscore_result.precision:.4f}")
            print(f"Recall:    {self.bertscore_result.recall:.4f}")
            print(f"F1 Score:  {self.bertscore_result.f1:.4f} (raw)")
            print(f"Scaled Score (0-10): {self.bertscore_result.normalized_score:.2f}")
            if self.bertscore_result.details.get("interpretation"):
                print(f"Interpretation: {self.bertscore_result.details['interpretation']}")
        
        # ROUGE-1
        print("\n" + "-"*80)
        print("ROUGE-1 (Lexical Overlap)")
        print("-"*80)
        print(f"Precision: {self.rouge_result.precision:.4f}")
        print(f"Recall:    {self.rouge_result.recall:.4f}")
        print(f"F1 Score:  {self.rouge_result.f1:.4f} (raw)")
        
        # For analysis tasks, show recall prominently
        if self.task_type == "analysis":
            print(f"\n📊 Coverage Score (Recall): {self.rouge_result.recall:.4f}")
            print(f"   → {self.rouge_result.recall*100:.1f}% of input content covered")
            print(f"\n📊 Scaled Score (0-10): {self.rouge_result.normalized_score:.2f}")
            print(f"   (Based on recall for analysis tasks)")
        else:
            print(f"Scaled Score (0-10): {self.rouge_result.normalized_score:.2f}")
        
        # Granular
        print("\n" + "-"*80)
        print("GRANULAR ANALYSIS")
        print("-"*80)
        
        # Highlight important metrics for analysis tasks
        if "key_term_preservation" in self.granular_analysis:
            ktp = self.granular_analysis["key_term_preservation"]
            print(f"\n🔑 KEY TERM PRESERVATION:")
            print(f"  ✓ Preserved: {ktp['preserved_terms_count']}/{ktp['total_key_terms']} terms ({ktp['preservation_rate']*100:.1f}%)")
            if ktp['preservation_rate'] >= 0.75:
                print(f"  ✅ Excellent preservation")
            elif ktp['preservation_rate'] >= 0.60:
                print(f"  ⚠️  Good, but some key terms missing")
            else:
                print(f"  ❌ Poor preservation - critical terms missing")
        
        if "token_statistics" in self.granular_analysis:
            ts = self.granular_analysis["token_statistics"]
            print(f"\n📝 TOKEN COVERAGE:")
            print(f"  Overlap ratio: {ts['token_overlap_ratio']:.3f}")
            if ts['token_overlap_ratio'] >= 0.70:
                print(f"  ✅ Strong coverage")
            elif ts['token_overlap_ratio'] >= 0.50:
                print(f"  ⚠️  Moderate coverage")
            else:
                print(f"  ❌ Weak coverage")
        
        if "length_analysis" in self.granular_analysis:
            la = self.granular_analysis["length_analysis"]
            print(f"\n📏 LENGTH ANALYSIS:")
            print(f"  Length ratio: {la['length_ratio']:.2f}x")
            print(f"  Assessment: {la['length_assessment']}")
        
        if "sentence_statistics" in self.granular_analysis:
            ss = self.granular_analysis["sentence_statistics"]
            print(f"\n📄 SENTENCE STATISTICS:")
            print(f"  Original: {ss['original_sentence_count']} sentences")
            print(f"  Generated: {ss['generated_sentence_count']} sentences")
        
        print("\n" + "="*80)
        print("OVERALL ASSESSMENT")
        print("="*80)
        for metric, score in self.summary["scores_by_metric"].items():
            print(f"{metric}: {score:.2f} / 10")
        
        if self.summary.get("final_grade"):
            print(f"\n{self.summary['final_grade']}")
        
        print("\n📌 Notes:")
        for note in self.summary.get("notes", []):
            print(f"  • {note}")
        print("="*80 + "\n")


class SemanticEvaluator:
    def __init__(self, original_text: str, generated_text: str, task_type: str = "analysis"):
        self.original_text = original_text
        self.generated_text = generated_text
        self.task_type = task_type  # "analysis", "summarization", "paraphrase"
        self.timestamp = datetime.now().isoformat()

    def evaluate(self,
                 use_bertscore: bool = True,
                 bertscore_model: str = "microsoft/deberta-large-mnli",
                 use_stemmer: bool = True,
                 device: Optional[str] = None) -> ComprehensiveReport:
        """
        Run task-appropriate evaluation.
        
        For analysis tasks:
        - BERTScore scaled [0.3, 0.85] → [0, 10] (wider acceptable range)
        - ROUGE-1 uses RECALL scaled to [0, 10] (not F1)
        - Length expansion is expected and positive
        """
        # BERTScore
        bertscore_result = None
        if use_bertscore:
            if self.task_type == "analysis":
                # Wider acceptable range for analysis
                bertscore_result = self._compute_bertscore_analysis(
                    model_type=bertscore_model,
                    device=device
                )
            else:
                # Standard range for summarization/paraphrase
                bertscore_result = self._compute_bertscore_standard(
                    model_type=bertscore_model,
                    device=device
                )

        # ROUGE-1
        if self.task_type == "analysis":
            rouge_result = self._compute_rouge_analysis(use_stemmer=use_stemmer)
        else:
            rouge_result = self._compute_rouge_standard(use_stemmer=use_stemmer)

        # Granular analysis
        granular_analysis = self._granular_analysis()

        # Summary
        summary = self._build_summary(bertscore_result, rouge_result, granular_analysis)

        return ComprehensiveReport(
            timestamp=self.timestamp,
            task_type=self.task_type,
            original_text_length=len(self.original_text),
            generated_text_length=len(self.generated_text),
            bertscore_result=bertscore_result,
            rouge_result=rouge_result,
            granular_analysis=granular_analysis,
            summary=summary
        )

    def _compute_bertscore_analysis(self, model_type: str, device: Optional[str]) -> EvaluationResult:
        """
        BERTScore for ANALYSIS tasks.
        Scaling: [0.3, 0.85] → [0, 10]
        Rationale: Analysis adds structure, so perfect alignment is rare.
        """
        try:
            from bert_score import score
            P, R, F1 = score(
                [self.generated_text],
                [self.original_text],
                lang="en",
                model_type=model_type,
                verbose=False,
                device=device
            )
            precision = P.item()
            recall = R.item()
            f1 = F1.item()

            # Analysis-appropriate scaling
            normalized = self._linear_scale(f1, in_min=0.30, in_max=0.85, out_min=0.0, out_max=10.0)

            return EvaluationResult(
                metric_name="BERTScore",
                precision=precision,
                recall=recall,
                f1=f1,
                raw_score=f1,
                normalized_score=normalized,
                details={
                    "model_used": model_type,
                    "scale_range": [0.30, 0.85],
                    "interpretation": self._interpret_bertscore_analysis(f1),
                    "note": "Scaled for analysis tasks (wider acceptable range)"
                }
            )
        except ImportError:
            return EvaluationResult(
                metric_name="BERTScore",
                precision=0.0, recall=0.0, f1=0.0, raw_score=0.0, normalized_score=0.0,
                details={"error": "bert_score package not installed"}
            )
        except Exception as e:
            return EvaluationResult(
                metric_name="BERTScore",
                precision=0.0, recall=0.0, f1=0.0, raw_score=0.0, normalized_score=0.0,
                details={"error": str(e)}
            )

    def _compute_bertscore_standard(self, model_type: str, device: Optional[str]) -> EvaluationResult:
        """BERTScore for summarization/paraphrase: [0.5, 1.0] → [0, 10]"""
        try:
            from bert_score import score
            P, R, F1 = score(
                [self.generated_text],
                [self.original_text],
                lang="en",
                model_type=model_type,
                verbose=False,
                device=device
            )
            precision = P.item()
            recall = R.item()
            f1 = F1.item()

            normalized = self._linear_scale(f1, in_min=0.50, in_max=1.0, out_min=0.0, out_max=10.0)

            return EvaluationResult(
                metric_name="BERTScore",
                precision=precision,
                recall=recall,
                f1=f1,
                raw_score=f1,
                normalized_score=normalized,
                details={
                    "model_used": model_type,
                    "scale_range": [0.50, 1.0],
                    "interpretation": self._interpret_bertscore_standard(f1)
                }
            )
        except Exception as e:
            return EvaluationResult(
                metric_name="BERTScore",
                precision=0.0, recall=0.0, f1=0.0, raw_score=0.0, normalized_score=0.0,
                details={"error": str(e)}
            )

    def _compute_rouge_analysis(self, use_stemmer: bool) -> EvaluationResult:
        """
        ROUGE-1 for ANALYSIS tasks.
        Uses RECALL (coverage) scaled directly to [0, 10].
        Rationale: Coverage of input content is most important.
        """
        try:
            from rouge_score import rouge_scorer
            scorer = rouge_scorer.RougeScorer(['rouge1'], use_stemmer=use_stemmer)
            scores = scorer.score(self.original_text, self.generated_text)
            rouge1 = scores['rouge1']
            
            precision = rouge1.precision
            recall = rouge1.recall
            f1 = rouge1.fmeasure
            
            # For analysis: use recall as primary metric
            normalized = recall * 10.0
            
            return EvaluationResult(
                metric_name="ROUGE-1",
                precision=precision,
                recall=recall,
                f1=f1,
                raw_score=recall,  # Store recall as raw score
                normalized_score=normalized,
                details={
                    "use_stemmer": use_stemmer,
                    "scaling_basis": "recall (coverage)",
                    "interpretation": self._interpret_rouge_recall(recall),
                    "note": "Recall prioritized for analysis tasks"
                }
            )
        except Exception as e:
            return EvaluationResult(
                metric_name="ROUGE-1",
                precision=0.0, recall=0.0, f1=0.0, raw_score=0.0, normalized_score=0.0,
                details={"error": str(e)}
            )

    def _compute_rouge_standard(self, use_stemmer: bool) -> EvaluationResult:
        """ROUGE-1 for summarization: uses F1 scaled to [0, 10]"""
        try:
            from rouge_score import rouge_scorer
            scorer = rouge_scorer.RougeScorer(['rouge1'], use_stemmer=use_stemmer)
            scores = scorer.score(self.original_text, self.generated_text)
            rouge1 = scores['rouge1']
            
            precision = rouge1.precision
            recall = rouge1.recall
            f1 = rouge1.fmeasure
            normalized = f1 * 10.0
            
            return EvaluationResult(
                metric_name="ROUGE-1",
                precision=precision,
                recall=recall,
                f1=f1,
                raw_score=f1,
                normalized_score=normalized,
                details={
                    "use_stemmer": use_stemmer,
                    "scaling_basis": "f1",
                    "interpretation": self._interpret_rouge_f1(f1)
                }
            )
        except Exception as e:
            return EvaluationResult(
                metric_name="ROUGE-1",
                precision=0.0, recall=0.0, f1=0.0, raw_score=0.0, normalized_score=0.0,
                details={"error": str(e)}
            )

    # Granular analysis (same as before)
    def _granular_analysis(self) -> Dict:
        analysis = {}

        original_tokens = self._simple_tokenize(self.original_text)
        generated_tokens = self._simple_tokenize(self.generated_text)
        original_unique = set(original_tokens)
        generated_unique = set(generated_tokens)
        common_tokens = original_unique & generated_unique
        missing_tokens = original_unique - generated_unique
        extra_tokens = generated_unique - original_unique

        analysis["token_statistics"] = {
            "original_token_count": len(original_tokens),
            "generated_token_count": len(generated_tokens),
            "original_unique_count": len(original_unique),
            "generated_unique_count": len(generated_unique),
            "common_tokens_count": len(common_tokens),
            "missing_tokens_count": len(missing_tokens),
            "extra_tokens_count": len(extra_tokens),
            "token_overlap_ratio": (len(common_tokens) / len(original_unique)) if original_unique else 0.0
        }

        key_terms = self._extract_key_terms(self.original_text)
        preserved_terms = [term for term in key_terms if term.lower() in self.generated_text.lower()]
        missing_terms = [term for term in key_terms if term.lower() not in self.generated_text.lower()]

        analysis["key_term_preservation"] = {
            "total_key_terms": len(key_terms),
            "preserved_terms_count": len(preserved_terms),
            "missing_terms_count": len(missing_terms),
            "preservation_rate": (len(preserved_terms) / len(key_terms)) if key_terms else 0.0,
            "preserved_terms": preserved_terms[:20],  # Top 20 for readability
            "missing_terms": missing_terms
        }

        length_ratio = (len(self.generated_text) / len(self.original_text)) if self.original_text else 0.0
        analysis["length_analysis"] = {
            "original_length": len(self.original_text),
            "generated_length": len(self.generated_text),
            "length_ratio": length_ratio,
            "length_assessment": self._assess_length_ratio_analysis(length_ratio) if self.task_type == "analysis" else self._assess_length_ratio_standard(length_ratio)
        }

        original_sentences = self._split_sentences(self.original_text)
        generated_sentences = self._split_sentences(self.generated_text)
        analysis["sentence_statistics"] = {
            "original_sentence_count": len(original_sentences),
            "generated_sentence_count": len(generated_sentences),
            "avg_original_sentence_length": float(np.mean([len(s.split()) for s in original_sentences])) if original_sentences else 0.0,
            "avg_generated_sentence_length": float(np.mean([len(s.split()) for s in generated_sentences])) if generated_sentences else 0.0
        }

        return analysis

    # Utility functions
    @staticmethod
    def _simple_tokenize(text: str) -> List[str]:
        return re.findall(r"[a-zA-Z0-9_]+", text.lower())

    @staticmethod
    def _split_sentences(text: str) -> List[str]:
        sentences = re.split(r'[.!?]+', text)
        return [s.strip() for s in sentences if s.strip()]

    @staticmethod
    def _linear_scale(value: float, in_min: float, in_max: float, out_min: float, out_max: float) -> float:
        if in_max <= in_min:
            return out_min
        clipped = max(in_min, min(in_max, value))
        scaled = (clipped - in_min) / (in_max - in_min)
        return out_min + scaled * (out_max - out_min)

    @staticmethod
    def _interpret_bertscore_analysis(f1: float) -> str:
        """Interpretation for analysis tasks"""
        if f1 >= 0.75: return "Excellent semantic alignment"
        if f1 >= 0.65: return "Very good semantic alignment"
        if f1 >= 0.55: return "Good semantic alignment"
        if f1 >= 0.45: return "Moderate semantic alignment"
        if f1 >= 0.35: return "Fair semantic alignment"
        return "Low semantic alignment"

    @staticmethod
    def _interpret_bertscore_standard(f1: float) -> str:
        """Interpretation for summarization/paraphrase"""
        if f1 >= 0.95: return "Excellent semantic similarity"
        if f1 >= 0.90: return "Very good semantic similarity"
        if f1 >= 0.85: return "Good semantic similarity"
        if f1 >= 0.80: return "Moderate semantic similarity"
        if f1 >= 0.70: return "Fair semantic similarity"
        return "Low semantic similarity"

    @staticmethod
    def _interpret_rouge_recall(recall: float) -> str:
        """Interpretation based on recall (coverage)"""
        if recall >= 0.90: return "Excellent coverage of input"
        if recall >= 0.80: return "Very good coverage of input"
        if recall >= 0.70: return "Good coverage of input"
        if recall >= 0.60: return "Moderate coverage of input"
        if recall >= 0.50: return "Fair coverage of input"
        return "Low coverage of input"

    @staticmethod
    def _interpret_rouge_f1(f1: float) -> str:
        """Interpretation based on F1"""
        if f1 >= 0.70: return "Excellent lexical overlap"
        if f1 >= 0.60: return "Very good lexical overlap"
        if f1 >= 0.50: return "Good lexical overlap"
        if f1 >= 0.40: return "Moderate lexical overlap"
        if f1 >= 0.30: return "Fair lexical overlap"
        return "Low lexical overlap"

    @staticmethod
    def _assess_length_ratio_analysis(ratio: float) -> str:
        """Length assessment for ANALYSIS tasks"""
        if ratio < 1.0:
            return "⚠️  Too brief - analysis should expand on input"
        elif ratio < 2.0:
            return "✅ Concise analysis (1-2x expansion)"
        elif ratio <= 4.0:
            return "✅ Detailed analysis (2-4x expansion - typical)"
        elif ratio <= 6.0:
            return "⚠️  Very detailed (4-6x expansion - acceptable)"
        else:
            return "❌ Extremely verbose (>6x expansion - may have redundancy)"

    @staticmethod
    def _assess_length_ratio_standard(ratio: float) -> str:
        """Length assessment for summarization/paraphrase"""
        if ratio < 0.5:
            return "Too brief - may be missing content"
        elif ratio < 0.8:
            return "Concise - good summarization"
        elif ratio <= 1.5:
            return "Appropriate length"
        elif ratio <= 2.5:
            return "Detailed - adds explanatory content"
        else:
            return "Very verbose - may contain redundancy"

    @staticmethod
    def _extract_key_terms(text: str) -> List[str]:
        key_terms = set()
        # Capitalized words
        key_terms.update(re.findall(r'\b[A-Z][a-zA-Z0-9_]+\b', text))
        # File patterns
        key_terms.update(re.findall(r'\*\.\w+|\b\w+\.\w+\b', text))
        # Ports
        ports = re.findall(r'port\s+(\d+)', text, flags=re.IGNORECASE) + re.findall(r':(\d{2,5})\b', text)
        key_terms.update([f"port {p}" for p in ports])
        # Docker images
        key_terms.update(re.findall(r'[a-z0-9/_\-]+:[\w\.\-]+', text))
        # Technical keywords
        for term in ['pipeline','workflow','dag','docker','json','csv','reconciliation','enrichment','transformation','api','network','volume']:
            if term in text.lower():
                key_terms.add(term)
        return list(key_terms)

    def _build_summary(self,
                       bertscore_result: Optional[EvaluationResult],
                       rouge_result: EvaluationResult,
                       granular_analysis: Dict) -> Dict:
        scores_by_metric = {}
        notes = []

        if bertscore_result:
            scores_by_metric['BERTScore'] = round(bertscore_result.normalized_score, 2)
            if 'error' not in bertscore_result.details:
                notes.append(f"BERTScore raw F1: {bertscore_result.f1:.4f}")

        scores_by_metric['ROUGE-1 (Coverage)'] = round(rouge_result.normalized_score, 2)
        notes.append(f"ROUGE-1 recall (coverage): {rouge_result.recall:.4f}")

        # Key metrics
        term_preservation = granular_analysis.get("key_term_preservation", {}).get("preservation_rate", 0.0)
        token_overlap = granular_analysis.get("token_statistics", {}).get("token_overlap_ratio", 0.0)
        
        notes.append(f"Key term preservation: {term_preservation:.3f}")
        notes.append(f"Token overlap: {token_overlap:.3f}")

        # Overall grade for analysis tasks
        if self.task_type == "analysis":
            avg_score = np.mean(list(scores_by_metric.values()))
            if avg_score >= 8.0:
                grade = "🏆 EXCELLENT - Strong semantic alignment and coverage"
            elif avg_score >= 6.5:
                grade = "✅ GOOD - Acceptable alignment with minor gaps"
            elif avg_score >= 5.0:
                grade = "⚠️  FAIR - Moderate alignment, review recommended"
            else:
                grade = "❌ POOR - Significant alignment issues"
            
            # Adjust grade based on key preservation
            if term_preservation >= 0.75:
                notes.append("✅ Strong key term preservation")
            else:
                grade += " (⚠️  low key term preservation)"
                notes.append("⚠️  Some critical terms missing")

        else:
            grade = None

        return {
            "scores_by_metric": scores_by_metric,
            "final_grade": grade,
            "notes": notes
        }


# CLI
def read_text_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def guess_device() -> Optional[str]:
    try:
        import torch
        return "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:
        return None

def main():
    parser = argparse.ArgumentParser(
        description="Semantic evaluation for analysis tasks (Step 1)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--original", required=True, help="Path to original prompt file")
    parser.add_argument("--generated", required=True, help="Path to generated analysis file")
    parser.add_argument("--task-type", default="analysis", choices=["analysis", "summarization", "paraphrase"],
                        help="Type of generation task")
    parser.add_argument("--use-bertscore", action="store_true", help="Compute BERTScore")
    parser.add_argument("--bertscore-model", default="microsoft/deberta-large-mnli", help="BERTScore model")
    parser.add_argument("--no-stemmer", action="store_true", help="Disable stemming for ROUGE-1")
    parser.add_argument("--device", default=None, help="Device for BERTScore (cuda|cpu)")
    parser.add_argument("--output-json", default=None, help="Path to save JSON report")
    parser.add_argument("--no-print", action="store_true", help="Do not print report to stdout")

    args = parser.parse_args()

    original_text = read_text_file(args.original)
    generated_text = read_text_file(args.generated)

    evaluator = SemanticEvaluator(original_text, generated_text, task_type=args.task_type)

    device = args.device if args.device else guess_device()

    report = evaluator.evaluate(
        use_bertscore=args.use_bertscore,
        bertscore_model=args.bertscore_model,
        use_stemmer=not args.no_stemmer,
        device=device
    )

    if not args.no_print:
        report.print_report()

    out_path = args.output_json
    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"evaluation_report_analysis_{ts}.json"
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    
    if not args.no_print:
        print(f"Report saved to {out_path}")


if __name__ == "__main__":
    main()