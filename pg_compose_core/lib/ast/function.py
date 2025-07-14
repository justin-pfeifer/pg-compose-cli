from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pg_compose_core.lib.ast.objects import ASTObject, BuildStage

@dataclass
class FunctionParameter:
    """Represents a function parameter."""
    name: str
    data_type: str
    mode: Optional[str] = None  # IN, OUT, INOUT, VARIADIC
    default_value: Optional[str] = None

@dataclass
class FunctionASTObject(ASTObject):
    """
    Extended ASTObject specifically for functions with additional function metadata.
    """
    parameters: List[FunctionParameter] = field(default_factory=list)
    return_type: Optional[str] = None
    language: Optional[str] = None
    volatility: Optional[str] = None
    security: Optional[str] = None  # DEFINER, INVOKER
    is_aggregate: bool = False
    is_window: bool = False
    is_leakproof: bool = False
    parallel: Optional[str] = None
    function_body: Optional[str] = None
    signature_hash: Optional[str] = None

    def __post_init__(self):
        self.query_type = BuildStage.FUNCTION
        super().__post_init__()
        if self.signature_hash is None:
            self.signature_hash = self._generate_signature_hash()

    def _generate_signature_hash(self) -> str:
        import hashlib
        signature_parts = []
        for param in self.parameters:
            param_str = f"{param.name}:{param.data_type}"
            if param.mode:
                param_str = f"{param.mode} {param_str}"
            signature_parts.append(param_str)
        if self.return_type:
            signature_parts.append(f"RETURNS {self.return_type}")
        if self.language:
            signature_parts.append(f"LANGUAGE {self.language}")
        signature = "|".join(signature_parts)
        return hashlib.sha256(signature.encode()).hexdigest()

    def signature_matches(self, other: 'FunctionASTObject') -> bool:
        return self.signature_hash == other.signature_hash

    def get_signature_sql(self) -> str:
        parts = []
        func_name = self.qualified_name
        param_strs = []
        for param in self.parameters:
            param_str = f"{param.name} {param.data_type}"
            if param.mode:
                param_str = f"{param.mode} {param_str}"
            param_strs.append(param_str)
        parts.append(f"CREATE OR REPLACE FUNCTION {func_name}({', '.join(param_strs)})")
        if self.return_type:
            parts.append(f"RETURNS {self.return_type}")
        return " ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        base_dict = super().to_dict()
        base_dict.update({
            "parameters": [{"name": p.name, "data_type": p.data_type, "mode": p.mode, "default_value": p.default_value} for p in self.parameters],
            "return_type": self.return_type,
            "language": self.language,
            "volatility": self.volatility,
            "security": self.security,
            "is_aggregate": self.is_aggregate,
            "is_window": self.is_window,
            "is_leakproof": self.is_leakproof,
            "parallel": self.parallel
        })
        return base_dict

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FunctionASTObject':
        params_data = data.get("parameters", [])
        parameters = [FunctionParameter(**p) for p in params_data]
        base_obj = ASTObject.from_dict(data)
        return cls(
            command=base_obj.command,
            object_name=base_obj.object_name,
            query_type=base_obj.query_type,
            resource_type=base_obj.resource_type,
            dependencies=base_obj.dependencies,
            query_hash=base_obj.query_hash,
            query_start_pos=base_obj.query_start_pos,
            query_end_pos=base_obj.query_end_pos,
            schema=base_obj.schema,
            ast_node=base_obj.ast_node,
            parameters=parameters,
            return_type=data.get("return_type"),
            language=data.get("language"),
            volatility=data.get("volatility"),
            security=data.get("security"),
            is_aggregate=data.get("is_aggregate", False),
            is_window=data.get("is_window", False),
            is_leakproof=data.get("is_leakproof", False),
            parallel=data.get("parallel")
        )

    def __str__(self) -> str:
        param_str = f"({', '.join(f'{p.name} {p.data_type}' for p in self.parameters)})" if self.parameters else "()"
        return f"FunctionASTObject({self.object_name or 'unnamed'}{param_str})"

    def __repr__(self) -> str:
        return (f"FunctionASTObject(command='{self.command[:50]}...', "
                f"object_name='{self.object_name}', "
                f"parameters={len(self.parameters)}, "
                f"return_type='{self.return_type}', "
                f"language='{self.language}', "
                f"dependencies={self.dependencies})") 