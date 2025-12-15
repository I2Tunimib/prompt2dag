#!/usr/bin/env python3
"""
Direct Prompting with Reasoning Models for DAG Generation
==========================================================

Generates Airflow/Prefect/Dagster DAGs using reasoning models (DeepSeek-R1, QwQ, etc.)
that include thinking/reasoning in their outputs.

Uses heuristic-based code extraction to avoid contamination from secondary LLMs.
"""

import json
import logging
import argparse
import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass

# Add parent directory to path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.append(parent_dir)
except NameError:
    project_root = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    if project_root not in sys.path:
        sys.path.append(project_root)


@dataclass
class ExtractionResult:
    """Result of code extraction from LLM response."""
    code: str
    reasoning: str
    confidence: str  # 'high', 'medium', 'low'
    method: str      # Which extraction method succeeded
    warnings: List[str]


class CodeExtractor:
    """
    Robust heuristic-based code extractor for reasoning model outputs.
    
    Handles various reasoning formats:
    - <think></think> blocks
    - <reasoning></reasoning> blocks  
    - Markdown code blocks
    - Natural language transitions
    """
    
    # Reasoning block patterns
    REASONING_PATTERNS = [
        (r'<think>(.*?)</think>', 'think_tags'),
        (r'<thinking>(.*?)</thinking>', 'thinking_tags'),
        (r'<reasoning>(.*?)</reasoning>', 'reasoning_tags'),
        (r'<analysis>(.*?)</analysis>', 'analysis_tags'),
        (r'\[REASONING\](.*?)\[/REASONING\]', 'reasoning_brackets'),
    ]
    
    # Code block patterns (in order of specificity)
    CODE_BLOCK_PATTERNS = [
        (r'```python\s*\n(.*?)\n```', 'python_markdown'),
        (r'```py\s*\n(.*?)\n```', 'py_markdown'),
        (r'```\s*\n(.*?)\n```', 'generic_markdown'),
    ]
    
    # Transition phrases that often precede code
    CODE_TRANSITIONS = [
        r"here'?s?\s+the\s+(?:complete|final|full)?\s*(?:code|implementation|solution|dag)",
        r"final\s+(?:code|implementation|solution|dag)",
        r"complete\s+(?:code|implementation|solution|dag)",
        r"the\s+(?:complete|final|full)\s+(?:code|implementation|solution|dag)",
        r"below\s+is\s+the\s+(?:complete|final|full)?\s*(?:code|implementation|solution|dag)",
        r"(?:python|airflow|prefect|dagster)\s+(?:code|implementation)",
    ]
    
    # Python code indicators
    PYTHON_START_PATTERNS = [
        r'^\s*"""',           # Docstrings
        r"^\s*'''",           # Docstrings
        r'^\s*#.*coding',     # Encoding declarations
        r'^\s*from\s+\w+\s+import',
        r'^\s*import\s+\w+',
        r'^\s*@\w+',          # Decorators
        r'^\s*def\s+\w+',     # Functions
        r'^\s*class\s+\w+',   # Classes
        r'^\s*with\s+DAG',    # Airflow
        r'^\s*@flow',         # Prefect
        r'^\s*@job',          # Dagster
        r'^\s*@op',           # Dagster
    ]
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract(self, raw_response: str) -> ExtractionResult:
        """
        Extract code from reasoning model response using multiple heuristic strategies.
        
        Args:
            raw_response: Raw LLM response including reasoning and code
            
        Returns:
            ExtractionResult with extracted code and metadata
        """
        self.logger.info("Starting heuristic code extraction")
        warnings = []
        
        # Strategy 1: Extract reasoning blocks first
        reasoning_text, content_after_reasoning = self._extract_reasoning_blocks(raw_response)
        
        if reasoning_text:
            self.logger.info(f"Extracted reasoning block ({len(reasoning_text)} chars)")
            primary_content = content_after_reasoning
        else:
            self.logger.debug("No explicit reasoning blocks found")
            reasoning_text = ""
            primary_content = raw_response
        
        # Strategy 2: Try markdown code blocks
        code, method = self._extract_from_code_blocks(primary_content)
        if code:
            self.logger.info(f"Extracted code using method: {method}")
            return ExtractionResult(
                code=code,
                reasoning=reasoning_text,
                confidence='high',
                method=method,
                warnings=warnings
            )
        
        # Strategy 3: Look for code transition phrases
        code, method = self._extract_after_transition(primary_content)
        if code:
            self.logger.info(f"Extracted code using method: {method}")
            return ExtractionResult(
                code=code,
                reasoning=reasoning_text,
                confidence='medium',
                method=method,
                warnings=warnings
            )
        
        # Strategy 4: Find Python code by pattern matching
        code, method = self._extract_by_python_patterns(primary_content)
        if code:
            self.logger.info(f"Extracted code using method: {method}")
            confidence = 'high' if self._validate_python_code(code) else 'medium'
            return ExtractionResult(
                code=code,
                reasoning=reasoning_text,
                confidence=confidence,
                method=method,
                warnings=warnings
            )
        
        # Strategy 5: Fallback - return all content after reasoning
        self.logger.warning("All extraction strategies failed, using fallback")
        warnings.append("Using fallback extraction - code quality may be poor")
        return ExtractionResult(
            code=primary_content.strip(),
            reasoning=reasoning_text,
            confidence='low',
            method='fallback_full_content',
            warnings=warnings
        )
    
    def _extract_reasoning_blocks(self, content: str) -> Tuple[str, str]:
        """
        Extract reasoning blocks and return (reasoning_text, remaining_content).
        """
        for pattern, name in self.REASONING_PATTERNS:
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            if matches:
                self.logger.debug(f"Found reasoning block using pattern: {name}")
                reasoning = '\n\n'.join(matches)
                # Remove all reasoning blocks from content
                remaining = re.sub(pattern, '', content, flags=re.DOTALL | re.IGNORECASE)
                return reasoning.strip(), remaining.strip()
        
        return "", content
    
    def _extract_from_code_blocks(self, content: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract code from markdown code blocks.
        """
        for pattern, method in self.CODE_BLOCK_PATTERNS:
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                # If multiple blocks, take the longest (likely the main code)
                code = max(matches, key=len)
                self.logger.debug(f"Found {len(matches)} code block(s) using {method}")
                return code.strip(), method
        
        return None, None
    
    def _extract_after_transition(self, content: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract code after transition phrases like "Here's the code:".
        """
        for transition_pattern in self.CODE_TRANSITIONS:
            # Look for transition phrase followed by content
            pattern = rf'{transition_pattern}[:\s]*\n+(.*)'
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                potential_code = match.group(1).strip()
                
                # Remove any remaining markdown if present
                potential_code = re.sub(r'^```(?:python|py)?\s*\n?', '', potential_code)
                potential_code = re.sub(r'\n?```\s*$', '', potential_code)
                
                self.logger.debug(f"Found code after transition phrase: {transition_pattern}")
                return potential_code.strip(), 'transition_phrase'
        
        return None, None
    
    def _extract_by_python_patterns(self, content: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract code by finding the first Python code pattern.
        """
        lines = content.split('\n')
        code_start_index = -1
        
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            
            for pattern in self.PYTHON_START_PATTERNS:
                if re.match(pattern, line):
                    code_start_index = i
                    self.logger.debug(f"Found Python code start at line {i+1}: {pattern}")
                    break
            
            if code_start_index >= 0:
                break
        
        if code_start_index >= 0:
            code = '\n'.join(lines[code_start_index:]).strip()
            return code, 'python_pattern_match'
        
        return None, None
    
    def _validate_python_code(self, code: str) -> bool:
        """
        Validate that extracted content looks like Python code.
        """
        # Check for key Python indicators
        indicators = [
            'import ',
            'from ',
            'def ',
            'class ',
            'DAG(',
            '@flow',
            '@job',
            '@op',
            '@task',
        ]
        
        has_indicators = any(indicator in code for indicator in indicators)
        
        # Try to compile it
        try:
            compile(code, '<string>', 'exec')
            return True
        except SyntaxError:
            self.logger.warning("Extracted code has syntax errors")
            return has_indicators
    
    def clean_code(self, code: str) -> str:
        """
        Clean extracted code by removing common artifacts.
        """
        # Remove any leading/trailing markdown
        code = re.sub(r'^```(?:python|py)?\s*\n?', '', code)
        code = re.sub(r'\n?```\s*$', '', code)
        
        # Remove multiple blank lines
        code = re.sub(r'\n{3,}', '\n\n', code)
        
        # Ensure it ends with a newline
        if not code.endswith('\n'):
            code += '\n'
        
        return code


class ReasoningModelClient:
    """
    Direct client for reasoning models via OpenAI-compatible API.
    """
    
    def __init__(self, config: Dict, model_key: str):
        """
        Initialize reasoning model client.
        
        Args:
            config: Configuration dict from config_reasoning_llm.json
            model_key: Model key (e.g., 'DeepSeek_R1', 'QwQ_32B')
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Get model configuration
        provider_config = config['model_settings']['deepinfra']
        
        # Use specified model key or active model
        self.model_key = model_key or provider_config.get('active_model')
        
        if self.model_key not in provider_config['models']:
            raise ValueError(f"Model key '{self.model_key}' not found in config")
        
        self.model_config = provider_config['models'][self.model_key]
        self.model_name = self.model_config['model_name']
        self.api_key = self.model_config.get('api_key')
        self.base_url = self.model_config.get('base_url')
        self.max_tokens = self.model_config.get('max_tokens', 8000)
        self.temperature = self.model_config.get('temperature', 0)
        
        if not self.api_key:
            raise ValueError(f"API key not configured for model: {self.model_key}")
        
        # Initialize OpenAI client
        try:
            from openai import OpenAI
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
            self.logger.info(f"Initialized reasoning model client: {self.model_name}")
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai>=1.0.0")
    
    def generate(self, system_prompt: str, user_prompt: str) -> Tuple[str, Dict]:
        """
        Generate completion from reasoning model.
        
        Args:
            system_prompt: System prompt
            user_prompt: User prompt
            
        Returns:
            Tuple of (response_content, token_usage_dict)
        """
        self.logger.info(f"Calling reasoning model: {self.model_name}")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            
            content = response.choices[0].message.content
            
            # Extract token usage
            token_usage = {
                'input_tokens': 0,
                'output_tokens': 0,
                'total_tokens': 0
            }
            
            if hasattr(response, 'usage') and response.usage:
                token_usage['input_tokens'] = getattr(response.usage, 'prompt_tokens', 0)
                token_usage['output_tokens'] = getattr(response.usage, 'completion_tokens', 0)
                token_usage['total_tokens'] = getattr(response.usage, 'total_tokens', 0)
            
            self.logger.info(f"Response received: {token_usage['total_tokens']} tokens")
            
            return content, token_usage
            
        except Exception as e:
            self.logger.error(f"Error calling reasoning model: {e}")
            raise
    
    def get_model_info(self) -> Dict:
        """Get model information."""
        return {
            'provider': 'deepinfra',
            'model_key': self.model_key,
            'model_name': self.model_name,
            'base_url': self.base_url
        }


class ReasoningDAGGenerator:
    """
    Generate orchestrator DAGs using reasoning models.
    """
    
    ORCHESTRATOR_CONFIGS = {
        'airflow': {
            'name': 'Apache Airflow',
            'decorator': 'DAG',
            'task_decorator': 'task',
            'imports': 'airflow.models, airflow.operators, airflow.providers',
        },
        'prefect': {
            'name': 'Prefect',
            'decorator': '@flow',
            'task_decorator': '@task',
            'imports': 'prefect, prefect.task_runners',
        },
        'dagster': {
            'name': 'Dagster',
            'decorator': '@job',
            'task_decorator': '@op',
            'imports': 'dagster, dagster.core',
        }
    }
    
    def __init__(self, config: Dict, model_client: ReasoningModelClient, orchestrator: str = 'airflow'):
        """
        Initialize reasoning DAG generator.
        
        Args:
            config: Configuration dictionary
            model_client: ReasoningModelClient instance
            orchestrator: Target orchestrator ('airflow', 'prefect', 'dagster')
        """
        self.config = config
        self.model_client = model_client
        self.extractor = CodeExtractor()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if orchestrator not in self.ORCHESTRATOR_CONFIGS:
            raise ValueError(f"Unsupported orchestrator: {orchestrator}")
        
        self.orchestrator = orchestrator
        self.orchestrator_config = self.ORCHESTRATOR_CONFIGS[orchestrator]
        
        self.token_usage_history = {
            'generation': {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}
        }
    
    def generate_dag(
        self,
        pipeline_description: str,
        output_dir: Path,
        output_filename: Optional[str] = None
    ) -> Dict:
        """
        Generate and save DAG from pipeline description.
        
        Args:
            pipeline_description: Text description of the pipeline
            output_dir: Directory to save outputs
            output_filename: Optional custom filename
            
        Returns:
            Results dictionary with generation details
        """
        self.logger.info(f"Generating {self.orchestrator} DAG from description")
        
        # Build prompts
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(pipeline_description)
        
        # Generate from reasoning model
        raw_response, token_usage = self.model_client.generate(system_prompt, user_prompt)
        self.token_usage_history['generation'] = token_usage
        
        # Extract code using heuristics
        extraction_result = self.extractor.extract(raw_response)
        
        # Clean code
        clean_code = self.extractor.clean_code(extraction_result.code)
        
        # Log extraction metadata
        self.logger.info(f"Extraction confidence: {extraction_result.confidence}")
        self.logger.info(f"Extraction method: {extraction_result.method}")
        if extraction_result.warnings:
            for warning in extraction_result.warnings:
                self.logger.warning(f"Extraction warning: {warning}")
        
        # Validate code
        if not self._basic_validation(clean_code):
            self.logger.error("Generated code failed basic validation")
        
        # Save code
        dag_file = self._save_code(clean_code, output_dir, output_filename)
        
        # Save reasoning
        reasoning_file = self._save_reasoning(extraction_result.reasoning, output_dir, output_filename)
        
        # Compile results
        model_info = self.model_client.get_model_info()
        results = {
            'orchestrator': self.orchestrator,
            'model_provider': model_info['provider'],
            'model_name': model_info['model_name'],
            'model_key': model_info['model_key'],
            'dag_file': str(dag_file),
            'reasoning_file': str(reasoning_file) if reasoning_file else None,
            'token_usage': self.token_usage_history['generation'],
            'extraction_method': extraction_result.method,
            'extraction_confidence': extraction_result.confidence,
            'extraction_warnings': extraction_result.warnings,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save metadata
        metadata_file = self._save_metadata(results, output_dir, output_filename)
        results['metadata_file'] = str(metadata_file)
        
        return results
    
    def _build_system_prompt(self) -> str:
        """Build system prompt for the target orchestrator."""
        orch_name = self.orchestrator_config['name']
        
        return f"""You are an expert {orch_name} developer. Generate complete, production-ready DAG/workflow code that follows best practices.

Requirements:
1. Include all necessary imports
2. Add comprehensive docstrings
3. Implement proper error handling
4. Follow PEP 8 style guidelines
5. Use appropriate {orch_name} patterns and idioms
6. Make the code executable and ready to deploy

You may provide reasoning about your approach, but ensure the final Python code is complete and correct."""
    
    def _build_user_prompt(self, pipeline_description: str) -> str:
        """Build user prompt with pipeline description."""
        orch_name = self.orchestrator_config['name']
        decorator = self.orchestrator_config['decorator']
        
        is_docker = self.config.get('direct_prompting', {}).get('dag_settings', {}).get('docker_based', False)
        docker_hint = f"\n- Use DockerOperator/DockerContainer for tasks where applicable" if is_docker else ""
        
        return f"""Convert this pipeline description into a complete {orch_name} workflow:

Pipeline Description:
```
{pipeline_description}
```

Generate a complete Python file with:
- All necessary imports
- Proper configuration
- Task/op definitions using {decorator}
- Correct dependency wiring{docker_hint}
- Error handling and retries where appropriate

Provide your reasoning if needed, then output the complete, executable Python code."""
    
    def _basic_validation(self, code: str) -> bool:
        """
        Perform basic validation on generated code.
        """
        # Check for orchestrator-specific patterns
        orch_indicators = {
            'airflow': ['DAG', 'import airflow'],
            'prefect': ['@flow', '@task', 'import prefect'],
            'dagster': ['@job', '@op', 'import dagster']
        }
        
        required = orch_indicators.get(self.orchestrator, [])
        has_required = all(indicator in code for indicator in required)
        
        if not has_required:
            self.logger.warning(f"Code missing required {self.orchestrator} patterns")
        
        # Try to compile
        try:
            compile(code, '<string>', 'exec')
            return True
        except SyntaxError as e:
            self.logger.error(f"Syntax error in generated code: {e}")
            return False
    
    def _save_code(
        self,
        code: str,
        output_dir: Path,
        filename: Optional[str] = None
    ) -> Path:
        """Save DAG code to file."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.orchestrator}_dag_reasoning_{timestamp}.py"
        elif not filename.endswith('.py'):
            filename += '.py'
        
        output_file = output_dir / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(code)
        
        self.logger.info(f"Code saved to: {output_file}")
        return output_file
    
    def _save_reasoning(
        self,
        reasoning: str,
        output_dir: Path,
        filename: Optional[str] = None
    ) -> Optional[Path]:
        """Save reasoning text to file."""
        if not reasoning:
            return None
        
        if filename:
            reasoning_filename = filename.replace('.py', '_reasoning.txt')
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            reasoning_filename = f"{self.orchestrator}_reasoning_{timestamp}.txt"
        
        reasoning_file = output_dir / reasoning_filename
        
        with open(reasoning_file, 'w', encoding='utf-8') as f:
            f.write(reasoning)
        
        self.logger.info(f"Reasoning saved to: {reasoning_file}")
        return reasoning_file
    
    def _save_metadata(
        self,
        results: Dict,
        output_dir: Path,
        filename: Optional[str] = None
    ) -> Path:
        """Save generation metadata."""
        if filename:
            metadata_filename = filename.replace('.py', '_metadata.json')
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_filename = f"{self.orchestrator}_metadata_{timestamp}.json"
        
        metadata_file = output_dir / metadata_filename
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Metadata saved to: {metadata_file}")
        return metadata_file


def load_config(config_path: str) -> Dict:
    """Load configuration file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate orchestrator DAGs using reasoning models (DeepSeek-R1, QwQ, etc.)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate Airflow DAG using DeepSeek-R1
  python direct_prompting_reasoning.py --input pipeline.txt --orchestrator airflow
  
  # Generate Prefect flow using QwQ-32B
  python direct_prompting_reasoning.py --input pipeline.txt --orchestrator prefect --model QwQ_32B
  
  # Generate Dagster job with custom output
  python direct_prompting_reasoning.py --input pipeline.txt --orchestrator dagster --output-filename my_job.py
        """
    )
    
    parser.add_argument(
        '--config',
        default='config_reasoning_llm.json',
        help='Path to reasoning model config (default: config_reasoning_llm.json)'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Path to pipeline description file (.txt)'
    )
    parser.add_argument(
        '--orchestrator',
        choices=['airflow', 'prefect', 'dagster'],
        default='airflow',
        help='Target orchestrator (default: airflow)'
    )
    parser.add_argument(
        '--output',
        default='outputs_reasoning',
        help='Output directory (default: outputs_reasoning)'
    )
    parser.add_argument(
        '--output-filename',
        help='Custom filename for generated code (e.g., my_dag.py)'
    )
    parser.add_argument(
        '--model',
        help='Model key to use (e.g., DeepSeek_R1, QwQ_32B, DeepSeek_R1_Turbo)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    try:
        # Load configuration
        logging.info(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        
        # Read pipeline description
        logging.info(f"Reading pipeline description from: {args.input}")
        with open(args.input, 'r', encoding='utf-8') as f:
            pipeline_description = f.read().strip()
        
        if not pipeline_description:
            logging.error("Pipeline description is empty")
            sys.exit(1)
        
        # Create output directory
        output_dir = Path(args.output) / args.orchestrator
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory: {output_dir.resolve()}")
        
        # Initialize reasoning model client
        logging.info("Initializing reasoning model client")
        model_client = ReasoningModelClient(config, args.model)
        model_info = model_client.get_model_info()
        logging.info(f"Using model: {model_info['model_name']} ({model_info['model_key']})")
        
        # Initialize DAG generator
        dag_generator = ReasoningDAGGenerator(
            config=config,
            model_client=model_client,
            orchestrator=args.orchestrator
        )
        
        # Generate DAG
        logging.info(f"Generating {args.orchestrator} DAG...")
        results = dag_generator.generate_dag(
            pipeline_description=pipeline_description,
            output_dir=output_dir,
            output_filename=args.output_filename
        )
        
        # Print summary
        print("\n" + "="*60)
        print("DAG GENERATION SUMMARY (Reasoning Model)")
        print("="*60)
        print(f"Orchestrator:           {results['orchestrator'].capitalize()}")
        print(f"Model:                  {results['model_name']}")
        print(f"Model Key:              {results['model_key']}")
        print(f"Extraction Method:      {results['extraction_method']}")
        print(f"Extraction Confidence:  {results['extraction_confidence']}")
        print(f"Token Usage:")
        print(f"  Input:                {results['token_usage']['input_tokens']}")
        print(f"  Output:               {results['token_usage']['output_tokens']}")
        print(f"  Total:                {results['token_usage']['total_tokens']}")
        print(f"Generated Files:")
        print(f"  Code:                 {results['dag_file']}")
        if results.get('reasoning_file'):
            print(f"  Reasoning:            {results['reasoning_file']}")
        print(f"  Metadata:             {results['metadata_file']}")
        
        if results.get('extraction_warnings'):
            print(f"\nWarnings:")
            for warning in results['extraction_warnings']:
                print(f"  - {warning}")
        
        print("="*60)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=args.debug)
        sys.exit(1)


if __name__ == "__main__":
    main()