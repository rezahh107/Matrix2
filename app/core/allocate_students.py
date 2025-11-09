"""
Implementation of student allocation to mentors based on eligibility matrix.
Complies with Policy v1.0.3 requirements for deterministic, safe allocation.
Fully optimized with vectorized operations, enhanced error reporting, and memory optimization.
"""
import copy
import math
from typing import Any, Dict, List, Optional, Tuple, Union
from pathlib import Path
import numpy as np

try:
    import pandas as pd
except ImportError:
    import pandas as pd


class OptimizedAllocationEngine:
    """Fully optimized engine for allocation operations with memory management."""
    
    @staticmethod
    def convert_columns_to_numeric(
        df: pd.DataFrame, 
        columns: List[str], 
        default: int = 0
    ) -> pd.DataFrame:
        """
        Vectorized conversion of multiple columns to numeric with memory optimization.
        """
        result_df = df.copy()
        
        for col in columns:
            if col in result_df.columns:
                # Use in-place operations where possible
                result_df[col] = pd.to_numeric(
                    result_df[col], 
                    errors='coerce'
                ).fillna(default, inplace=False).astype(int)
        
        return result_df
    
    @staticmethod
    def fast_capacity_lookup(
        candidate_pool: pd.DataFrame, 
        capacity_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Optimized capacity lookup using vectorized merge operations.
        """
        # Use merge for multiple columns - faster and more reliable
        capacity_subset = capacity_df[['پشتیبان', 'تعداد داوطلبان تحت پوشش', 'تعداد تحت پوشش خاص']].copy()
        
        merged = candidate_pool.merge(
            capacity_subset, 
            on='پشتیبان', 
            how='left',
            suffixes=('', '_y')
        )
        
        # Vectorized filling of missing values with in-place where possible
        merged['تعداد داوطلبان تحت پوشش'].fillna(0, inplace=True)
        merged['تعداد تحت پوشش خاص'].fillna(0, inplace=True)
        
        return merged
    
    @staticmethod
    def calculate_occupancy_ratios_vectorized(
        covered_now: np.ndarray, 
        special_limit: np.ndarray
    ) -> np.ndarray:
        """
        Vectorized occupancy ratio calculation using numpy.
        """
        # Avoid division by zero
        denominators = np.maximum(special_limit, 1)
        
        # Vectorized division with error handling
        with np.errstate(divide='ignore', invalid='ignore'):
            ratios = np.divide(covered_now, denominators)
            ratios = np.nan_to_num(ratios, nan=0.0, posinf=0.0, neginf=0.0)
        
        return ratios
    
    @staticmethod
    def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage by downcasting numeric types.
        """
        optimized_df = df.copy()
        
        # Downcast integer columns
        int_cols = optimized_df.select_dtypes(include=['int64']).columns
        for col in int_cols:
            optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='integer')
        
        # Downcast float columns  
        float_cols = optimized_df.select_dtypes(include=['float64']).columns
        for col in float_cols:
            optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
        
        return optimized_df


def safe_int_convert(value: Any, default: int = 0) -> int:
    """Convert value to int safely, returning default on failure."""
    try:
        if pd.isna(value):
            return default
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def build_allocation_matrix(eligibility_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds an optimized lookup structure for allocation.
    Uses fully vectorized operations for maximum performance.
    """
    # Ensure required columns exist
    required_cols = [
        "کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", 
        "مالی حکمت بنیاد", "کد مدرسه", "پشتیبان", "ردیف پشتیبان"
    ]
    
    missing_cols = [col for col in required_cols if col not in eligibility_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in eligibility matrix: {missing_cols}")
    
    # Use vectorized conversion for all numeric columns
    cols_to_convert = [
        col for col in [
            "کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", 
            "مالی حکمت بنیاد", "کد مدرسه", "ردیف پشتیبان"
        ] 
        if col in eligibility_df.columns
    ]
    
    # Vectorized conversion - much faster than apply
    eligibility_df = OptimizedAllocationEngine.convert_columns_to_numeric(
        eligibility_df, cols_to_convert, 0
    )
    
    # Create composite key for ultra-fast filtering
    key_columns = ["کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", 
                   "مالی حکمت بنیاد", "کد مدرسه"]
    
    # Vectorized composite key creation without apply
    composite_parts = []
    for col in key_columns:
        if col in eligibility_df.columns:
            composite_parts.append(eligibility_df[col].astype(str))
    
    if composite_parts:
        eligibility_df['_composite_key'] = composite_parts[0]
        for part in composite_parts[1:]:
            eligibility_df['_composite_key'] = eligibility_df['_composite_key'] + "_" + part
    else:
        eligibility_df['_composite_key'] = ""
    
    # Optimize memory usage
    eligibility_df = OptimizedAllocationEngine.optimize_dataframe_memory(eligibility_df)
    
    return eligibility_df


def validate_inputs_comprehensive(
    eligibility_df: pd.DataFrame,
    students_df: pd.DataFrame,
    capacity_df: pd.DataFrame
) -> Tuple[bool, List[str], List[str]]:
    """
    Comprehensive input validation with enhanced NaN value checking and data quality analysis.
    Returns (is_valid, warnings, errors)
    """
    warnings = []
    errors = []
    
    # Enhanced empty dataframe check with specific messages
    if eligibility_df.empty:
        errors.append("Eligibility matrix dataframe is empty - no mentor eligibility data available")
    
    if students_df.empty:
        errors.append("Students dataframe is empty - no students to allocate")
    
    if capacity_df.empty:
        errors.append("Capacity dataframe is empty - no mentor capacity data available")
    
    # If any critical dataframes are empty, return early
    if errors:
        return False, warnings, errors
    
    # Required columns check
    required_eligibility_cols = [
        "کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", 
        "مالی حکمت بنیاد", "کد مدرسه", "پشتیبان", "ردیف پشتیبان"
    ]
    
    missing_eligibility = [col for col in required_eligibility_cols if col not in eligibility_df.columns]
    if missing_eligibility:
        errors.append(f"Missing required columns in eligibility matrix: {missing_eligibility}")
    
    # Capacity columns check
    capacity_required = ["پشتیبان", "تعداد داوطلبان تحت پوشش", "تعداد تحت پوشش خاص"]
    missing_capacity = [col for col in capacity_required if col not in capacity_df.columns]
    if missing_capacity:
        errors.append(f"Missing required columns in capacity: {missing_capacity}")
    
    # Enhanced NaN value checking for critical columns
    critical_eligibility_cols = ["کدرشته", "جنسیت", "کد مدرسه", "پشتیبان"]
    for col in critical_eligibility_cols:
        if col in eligibility_df.columns:
            nan_count = eligibility_df[col].isna().sum()
            if nan_count > 0:
                errors.append(f"Found {nan_count} NaN values in critical eligibility column '{col}' - these will cause allocation failures")
    
    # Check for NaN in capacity critical columns
    capacity_critical_cols = ["پشتیبان", "تعداد تحت پوشش خاص"]
    for col in capacity_critical_cols:
        if col in capacity_df.columns:
            nan_count = capacity_df[col].isna().sum()
            if nan_count > 0:
                errors.append(f"Found {nan_count} NaN values in critical capacity column '{col}' - these will cause allocation failures")
    
    # Check student data for critical NaN values
    student_critical_cols = ["کدرشته", "جنسیت", "کد مدرسه"]
    for col in student_critical_cols:
        if col in students_df.columns:
            nan_count = students_df[col].isna().sum()
            if nan_count > 0:
                errors.append(f"Found {nan_count} NaN values in critical student column '{col}' - these students cannot be allocated")
    
    # Data quality warnings
    # Check for duplicate composite keys in eligibility matrix
    key_columns = ["کدرشته", "جنسیت", "دانش آموز فارغ", "مرکز گلستان صدرا", 
                  "مالی حکمت بنیاد", "کد مدرسه", "پشتیبان"]
    existing_keys = [col for col in key_columns if col in eligibility_df.columns]
    
    if existing_keys:
        duplicates = eligibility_df.duplicated(subset=existing_keys).sum()
        if duplicates > 0:
            warnings.append(f"Found {duplicates} duplicate mentor entries in eligibility matrix - this may indicate data quality issues")
    
    # Capacity data quality checks
    duplicate_mentors = capacity_df.duplicated('پشتیبان', keep=False).sum()
    if duplicate_mentors > 0:
        errors.append(f"Found {duplicate_mentors} duplicate mentor entries in capacity data - each mentor must have unique capacity record")
    
    zero_capacity = (capacity_df['تعداد تحت پوشش خاص'] == 0).sum()
    if zero_capacity > 0:
        warnings.append(f"Found {zero_capacity} mentors with zero special capacity - these mentors cannot accept new students")
    
    negative_capacity = (capacity_df['تعداد تحت پوشش خاص'] < 0).sum()
    if negative_capacity > 0:
        errors.append(f"Found {negative_capacity} mentors with negative capacity - please fix capacity data")
    
    # Student data validation
    required_student_fields = ["کدرشته", "جنسیت", "کد مدرسه"]
    missing_student_fields = [field for field in required_student_fields if field not in students_df.columns]
    if missing_student_fields:
        errors.append(f"Missing required student fields: {missing_student_fields} - allocation will fail for all students")
    
    # Check for invalid values in key numeric columns
    if 'کدرشته' in eligibility_df.columns:
        invalid_codes = (eligibility_df['کدرشته'] < 0).sum()
        if invalid_codes > 0:
            warnings.append(f"Found {invalid_codes} negative values in 'کدرشته' column")
    
    if 'جنسیت' in eligibility_df.columns:
        invalid_gender = (~eligibility_df['جنسیت'].isin([0, 1])).sum()
        if invalid_gender > 0:
            warnings.append(f"Found {invalid_gender} invalid gender values (should be 0 or 1)")
    
    is_valid = len(errors) == 0
    
    return is_valid, warnings, errors


def find_mentor_for_student(
    student: Dict[str, Any], 
    matrix_df: pd.DataFrame, 
    capacity_df: pd.DataFrame,
    allocation_priority_rules: Optional[Dict[str, Any]] = None
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Find the best mentor for a student using optimized vectorized operations.
    Enhanced with advanced priority rules and detailed failure analysis.
    """
    # Build student composite key for efficient matching
    student_key_values = {
        "کدرشته": safe_int_convert(student.get("کدرشته", 0)),
        "جنسیت": safe_int_convert(student.get("جنسیت", 0)),
        "دانش آموز فارغ": safe_int_convert(student.get("دانش آموز فارغ", 0)),
        "مرکز گلستان صدرا": safe_int_convert(student.get("مرکز گلستان صدرا", 0)),
        "مالی حکمت بنیاد": safe_int_convert(student.get("مالی حکمت بنیاد", 0)),
        "کد مدرسه": safe_int_convert(student.get("کد مدرسه", 0))
    }
    
    # Enhanced failure analysis - check for missing critical fields
    missing_critical_fields = []
    for field, value in student_key_values.items():
        if value == 0 and field in ["کدرشته", "جنسیت", "کد مدرسه"]:
            missing_critical_fields.append(field)
    
    if missing_critical_fields:
        return None, {
            "student_id": student.get("student_id", "unknown"),
            "key": student_key_values,
            "reason": "missing_critical_student_fields",
            "candidate_count": 0,
            "composite_key": "",
            "error_type": "INVALID_STUDENT_DATA",
            "detailed_reason": f"Student missing critical fields: {missing_critical_fields}",
            "missing_fields": missing_critical_fields,
            "student_data_quality": "poor"
        }
    
    student_composite_key = "_".join(str(v) for v in student_key_values.values())
    
    # Ultra-fast filtering using composite key
    candidate_mask = matrix_df['_composite_key'] == student_composite_key
    candidate_pool = matrix_df[candidate_mask].copy()
    
    if candidate_pool.empty:
        return None, {
            "student_id": student.get("student_id", "unknown"),
            "key": student_key_values,
            "reason": "no_candidate_mentors_found_for_student_profile",
            "candidate_count": 0,
            "composite_key": student_composite_key,
            "error_type": "ELIGIBILITY_NO_MATCH",
            "detailed_reason": "No mentors match the student's eligibility criteria based on the six join keys",
            "student_profile_analysis": analyze_student_profile(student_key_values),
            "suggested_mentor_criteria": suggest_mentor_criteria(student_key_values)
        }
    
    # Optimized capacity lookup using vectorized merge
    candidate_pool = OptimizedAllocationEngine.fast_capacity_lookup(candidate_pool, capacity_df)
    
    # Rename for clarity using vectorized assignment
    candidate_pool.rename(columns={
        'تعداد داوطلبان تحت پوشش': 'covered_now',
        'تعداد تحت پوشش خاص': 'special_limit'
    }, inplace=True)
    
    # Initialize allocations
    candidate_pool['allocations_new'] = 0
    
    # Vectorized occupancy ratio calculation
    covered_values = candidate_pool['covered_now'].values
    limit_values = candidate_pool['special_limit'].values
    
    candidate_pool['occupancy_ratio'] = OptimizedAllocationEngine.calculate_occupancy_ratios_vectorized(
        covered_values, limit_values
    )
    
    # Apply enhanced priority rules if provided
    if allocation_priority_rules:
        candidate_pool = apply_enhanced_priority_rules(
            candidate_pool, student, allocation_priority_rules
        )
    
    # Sort with optimized stable sort
    sort_columns = ["occupancy_ratio", "allocations_new", "ردیف پشتیبان"]
    sorted_pool = candidate_pool.sort_values(
        by=sort_columns,
        ascending=[True, True, True],
        kind="stable"
    ).reset_index(drop=True)
    
    if sorted_pool.empty:
        return None, {
            "student_id": student.get("student_id", "unknown"),
            "key": student_key_values,
            "reason": "all_eligible_mentors_at_full_capacity",
            "candidate_count": len(candidate_pool),
            "composite_key": student_composite_key,
            "capacity_issues": analyze_capacity_issues_detailed(candidate_pool),
            "error_type": "CAPACITY_FULL",
            "detailed_reason": "Mentors match eligibility but all are at or over capacity",
            "capacity_analysis": analyze_capacity_distribution(candidate_pool)
        }
    
    # Select best mentor
    best_mentor = sorted_pool.iloc[0].to_dict()
    
    # Enhanced capacity check with detailed analysis
    mentor_occupancy = best_mentor.get('occupancy_ratio', 0)
    if mentor_occupancy >= 1.0:
        return None, {
            "student_id": student.get("student_id", "unknown"),
            "key": student_key_values,
            "reason": "best_candidate_mentor_over_capacity",
            "candidate_count": len(candidate_pool),
            "composite_key": student_composite_key,
            "capacity_issues": analyze_capacity_issues_detailed(candidate_pool),
            "error_type": "CAPACITY_FULL",
            "detailed_reason": "Even the best matching mentor is at or over capacity",
            "best_candidate_occupancy": mentor_occupancy,
            "alternative_candidates": find_alternative_candidates(sorted_pool, student_key_values)
        }
    
    # Prepare comprehensive trace information
    trace = {
        "student_id": student.get("student_id", "unknown"),
        "key": student_key_values,
        "candidate_count": len(candidate_pool),
        "composite_key": student_composite_key,
        "selected_mentor": best_mentor.get("پشتیبان", "unknown"),
        "selection_reason": "lowest_occupancy_ratio_then_min_new_allocations_then_min_mentor_id",
        "allocation_score": round(best_mentor.get("occupancy_ratio", float('inf')), 4),
        "allocation_details": {
            "covered_now": best_mentor.get("covered_now", 0),
            "special_limit": best_mentor.get("special_limit", 0),
            "new_allocations": best_mentor.get("allocations_new", 0),
            "occupancy_percentage": round(best_mentor.get("occupancy_ratio", 0) * 100, 2),
            "remaining_capacity": max(0, best_mentor.get("special_limit", 0) - best_mentor.get("covered_now", 0))
        },
        "capacity_analysis": analyze_candidate_capacity_detailed(sorted_pool),
        "top_candidates_preview": get_top_candidates_preview(sorted_pool, 5),
        "allocation_quality": assess_enhanced_allocation_quality(best_mentor, student),
        "priority_rules_applied": allocation_priority_rules is not None
    }
    
    return best_mentor, trace


def apply_enhanced_priority_rules(
    candidate_pool: pd.DataFrame, 
    student: Dict[str, Any], 
    rules: Dict[str, Any]
) -> pd.DataFrame:
    """
    Apply enhanced priority rules using vectorized operations.
    Supports complex prioritization scenarios.
    """
    pool = candidate_pool.copy()
    
    # Initialize priority score
    pool['priority_score'] = 0
    
    # Priority rule: specific schools
    if 'priority_schools' in rules:
        priority_schools = rules['priority_schools']
        school_code = safe_int_convert(student.get('کد مدرسه', 0))
        
        # Vectorized priority scoring for schools
        pool['priority_score'] += np.where(
            pool['کد مدرسه'].isin(priority_schools), 2, 0
        )
        
        # Extra priority for exact school match
        pool['priority_score'] += np.where(
            pool['کد مدرسه'] == school_code, 3, 0
        )
    
    # Priority rule: new mentors (low allocation count)
    if rules.get('priority_new_mentors', False):
        # Prioritize mentors with fewer current allocations
        max_covered = pool['covered_now'].max()
        if max_covered > 0:
            # Normalize and invert (lower allocations = higher priority)
            allocation_priority = 1 - (pool['covered_now'] / max_covered)
            pool['priority_score'] += allocation_priority * 2
    
    # Priority rule: high capacity mentors
    if rules.get('priority_high_capacity', False):
        # Prioritize mentors with higher total capacity
        max_capacity = pool['special_limit'].max()
        if max_capacity > 0:
            capacity_priority = pool['special_limit'] / max_capacity
            pool['priority_score'] += capacity_priority
    
    # Priority rule: maximum occupancy threshold
    if 'max_occupancy_threshold' in rules:
        threshold = rules['max_occupancy_threshold']
        # Filter out mentors above threshold
        pool = pool[pool['occupancy_ratio'] <= threshold]
    
    # Priority rule: mentor experience or specialization
    if 'mentor_specializations' in rules:
        specializations = rules['mentor_specializations']
        # This would require additional mentor metadata columns
        # Example implementation:
        if 'تخصص' in pool.columns:
            for specialization, bonus in specializations.items():
                pool['priority_score'] += np.where(
                    pool['تخصص'] == specialization, bonus, 0
                )
    
    # Sort by priority score if any rules were applied
    if 'priority_score' in pool.columns and pool['priority_score'].sum() > 0:
        pool = pool.sort_values(
            by=['priority_score', 'occupancy_ratio', 'allocations_new', 'ردیف پشتیبان'],
            ascending=[False, True, True, True],
            kind='stable'
        )
    
    return pool


def analyze_student_profile(student_key: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze student profile for better failure reporting."""
    analysis = {
        "field_completeness": "complete",
        "unusual_values": [],
        "profile_characteristics": []
    }
    
    # Check for default/zero values that might indicate missing data
    for field, value in student_key.items():
        if value == 0 and field in ["کدرشته", "کد مدرسه"]:
            analysis["unusual_values"].append(f"{field} is zero")
            analysis["field_completeness"] = "incomplete"
    
    # Profile characteristics
    if student_key.get("مرکز گلستان صدرا", 0) == 1:
        analysis["profile_characteristics"].append("گلستان صدرا دانش آموز")
    
    if student_key.get("دانش آموز فارغ", 0) == 1:
        analysis["profile_characteristics"].append("فارغ التحصیل")
    
    if student_key.get("مالی حکمت بنیاد", 0) == 1:
        analysis["profile_characteristics"].append("بنیاد حکمت")
    
    return analysis


def suggest_mentor_criteria(student_key: Dict[str, Any]) -> List[str]:
    """Suggest mentor criteria based on student profile."""
    suggestions = []
    
    if student_key.get("کدرشته", 0) > 0:
        suggestions.append(f"Look for mentors supporting کدرشته: {student_key['کدرشته']}")
    
    if student_key.get("مرکز گلستان صدرا", 0) == 1:
        suggestions.append("Prioritize mentors specialized in گلستان صدرا students")
    
    if student_key.get("دانش آموز فارغ", 0) == 1:
        suggestions.append("Consider mentors experienced with فارغ التحصیل students")
    
    return suggestions


def analyze_capacity_distribution(candidate_pool: pd.DataFrame) -> Dict[str, Any]:
    """Analyze capacity distribution for detailed reporting."""
    if candidate_pool.empty:
        return {"message": "No candidates to analyze"}
    
    return {
        "capacity_tiers": {
            "high_capacity": len(candidate_pool[candidate_pool['special_limit'] > 20]),
            "medium_capacity": len(candidate_pool[(candidate_pool['special_limit'] >= 10) & (candidate_pool['special_limit'] <= 20)]),
            "low_capacity": len(candidate_pool[candidate_pool['special_limit'] < 10])
        },
        "utilization_patterns": {
            "under_utilized": len(candidate_pool[candidate_pool['occupancy_ratio'] < 0.5]),
            "well_utilized": len(candidate_pool[(candidate_pool['occupancy_ratio'] >= 0.5) & (candidate_pool['occupancy_ratio'] < 0.8)]),
            "over_utilized": len(candidate_pool[candidate_pool['occupancy_ratio'] >= 0.8])
        },
        "recommendations": generate_capacity_recommendations(candidate_pool)
    }


def find_alternative_candidates(candidate_pool: pd.DataFrame, student_key: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Find alternative candidates when primary choices are unavailable."""
    alternatives = []
    
    # Look for mentors with similar characteristics but different specialties
    similar_candidates = candidate_pool[
        (candidate_pool['occupancy_ratio'] < 1.0) &
        (candidate_pool['special_limit'] > candidate_pool['covered_now'])
    ].head(3)
    
    for idx, row in similar_candidates.iterrows():
        alternatives.append({
            "mentor": row.get("پشتیبان", ""),
            "occupancy_ratio": round(row.get("occupancy_ratio", 0), 3),
            "remaining_capacity": max(0, row.get("special_limit", 0) - row.get("covered_now", 0)),
            "reason": "available_capacity"
        })
    
    return alternatives


def assess_enhanced_allocation_quality(
    mentor_record: Dict[str, Any], 
    student: Dict[str, Any]
) -> Dict[str, Any]:
    """Enhanced quality assessment with multiple factors."""
    occupancy = mentor_record.get('occupancy_ratio', 1.0)
    remaining_capacity = max(0, mentor_record.get('special_limit', 0) - mentor_record.get('covered_now', 0))
    mentor_experience = mentor_record.get('تجربه', 0)  # Assuming experience data exists
    specialization_match = assess_specialization_match(mentor_record, student)
    
    # Base quality on occupancy
    if occupancy < 0.3:
        base_quality = "excellent"
        base_score = 10
    elif occupancy < 0.6:
        base_quality = "good"
        base_score = 7
    elif occupancy < 0.8:
        base_quality = "fair" 
        base_score = 5
    elif occupancy < 1.0:
        base_quality = "poor"
        base_score = 3
    else:
        base_quality = "critical"
        base_score = 0
    
    # Adjust score based on other factors
    final_score = base_score
    
    # Experience bonus
    if mentor_experience > 5:  # 5+ years experience
        final_score += 2
    elif mentor_experience > 2:  # 2-5 years experience
        final_score += 1
    
    # Specialization match bonus
    if specialization_match == "exact":
        final_score += 3
    elif specialization_match == "good":
        final_score += 1
    
    # Capacity bonus
    if remaining_capacity > 10:
        final_score += 2
    elif remaining_capacity > 5:
        final_score += 1
    
    # Determine final quality
    if final_score >= 12:
        final_quality = "excellent"
    elif final_score >= 9:
        final_quality = "good"
    elif final_score >= 6:
        final_quality = "fair"
    elif final_score >= 3:
        final_quality = "poor"
    else:
        final_quality = "critical"
    
    return {
        "quality_rating": final_quality,
        "quality_score": final_score,
        "base_occupancy_quality": base_quality,
        "occupancy_level": occupancy,
        "remaining_capacity": remaining_capacity,
        "experience_bonus": mentor_experience > 2,
        "specialization_match": specialization_match,
        "capacity_bonus": remaining_capacity > 5,
        "detailed_breakdown": {
            "base_score": base_score,
            "experience_bonus": 2 if mentor_experience > 5 else 1 if mentor_experience > 2 else 0,
            "specialization_bonus": 3 if specialization_match == "exact" else 1 if specialization_match == "good" else 0,
            "capacity_bonus": 2 if remaining_capacity > 10 else 1 if remaining_capacity > 5 else 0
        },
        "recommendation": generate_quality_recommendation(final_quality, occupancy, remaining_capacity)
    }


def assess_specialization_match(mentor_record: Dict[str, Any], student: Dict[str, Any]) -> str:
    """Assess how well mentor specialization matches student needs."""
    # This is a simplified implementation
    # In practice, you would have more detailed specialization data
    
    mentor_specialties = mentor_record.get('تخصص', '')
    student_needs = student.get('نیازهای ویژه', '')
    
    if not mentor_specialties or not student_needs:
        return "unknown"
    
    # Simple matching logic - extend based on actual data structure
    if student_needs in mentor_specialties:
        return "exact"
    elif any(need in mentor_specialties for need in student_needs.split(',')):
        return "good"
    else:
        return "poor"


def generate_quality_recommendation(quality: str, occupancy: float, remaining_capacity: int) -> str:
    """Generate specific recommendations based on allocation quality."""
    if quality == "excellent":
        return "Ideal allocation - mentor has ample capacity and good match"
    elif quality == "good":
        return "Good allocation - consider monitoring capacity usage"
    elif quality == "fair":
        return "Acceptable allocation - mentor capacity is becoming limited"
    elif quality == "poor":
        return "Poor allocation - mentor has very limited capacity, consider alternatives"
    else:  # critical
        return "Critical allocation - mentor is at or near full capacity, urgent review needed"


def generate_capacity_recommendations(candidate_pool: pd.DataFrame) -> List[str]:
    """Generate specific capacity management recommendations."""
    recommendations = []
    
    total_mentors = len(candidate_pool)
    full_capacity = len(candidate_pool[candidate_pool['occupancy_ratio'] >= 1.0])
    high_occupancy = len(candidate_pool[candidate_pool['occupancy_ratio'] >= 0.8])
    
    if full_capacity == total_mentors:
        recommendations.append("🚨 ALL mentors at full capacity - immediate expansion needed")
    elif full_capacity > total_mentors * 0.7:
        recommendations.append("🔴 Over 70% of mentors at full capacity - urgent action required")
    elif high_occupancy > total_mentors * 0.5:
        recommendations.append("🟡 Over 50% of mentors at high occupancy - capacity planning needed")
    
    # Check for capacity distribution issues
    capacity_std = candidate_pool['special_limit'].std()
    if capacity_std > candidate_pool['special_limit'].mean() * 0.5:
        recommendations.append("📊 High variance in mentor capacity - consider standardization")
    
    return recommendations


# Existing functions remain the same but with memory optimizations where applicable
def analyze_capacity_issues_detailed(candidate_pool: pd.DataFrame) -> Dict[str, Any]:
    """Detailed analysis of capacity issues for error reporting."""
    if candidate_pool.empty:
        return {
            "message": "No candidates available for allocation",
            "total_candidates": 0,
            "suggested_actions": ["Verify student eligibility criteria", "Check mentor coverage configuration"]
        }
    
    total = len(candidate_pool)
    full_capacity = len(candidate_pool[candidate_pool['occupancy_ratio'] >= 1.0])
    high_occupancy = len(candidate_pool[candidate_pool['occupancy_ratio'] >= 0.9])
    medium_occupancy = len(candidate_pool[candidate_pool['occupancy_ratio'] >= 0.7])
    low_occupancy = len(candidate_pool[candidate_pool['occupancy_ratio'] < 0.7])
    
    # Enhanced capacity statistics
    capacity_stats = {
        "total_candidates": total,
        "full_capacity_mentors": full_capacity,
        "high_occupancy_mentors": high_occupancy,
        "medium_occupancy_mentors": medium_occupancy,
        "low_occupancy_mentors": low_occupancy,
        "available_candidates": total - full_capacity,
        "occupancy_distribution": {
            "0-70%": low_occupancy,
            "70-90%": medium_occupancy,
            "90-100%": high_occupancy - full_capacity,
            "100%+": full_capacity
        },
        "mentors_near_capacity": len(candidate_pool[candidate_pool['occupancy_ratio'] >= 0.95]),
        "mentors_with_available_capacity": len(candidate_pool[candidate_pool['occupancy_ratio'] < 0.8])
    }
    
    # Enhanced suggested actions based on detailed analysis
    suggested_actions = []
    
    if full_capacity == total:
        suggested_actions.extend([
            "🚫 CRITICAL: All eligible mentors are at full capacity",
            "💡 Consider increasing mentor capacity limits immediately",
            "👥 Evaluate adding new mentors for this student profile",
            "⚖️ Review current allocation distribution"
        ])
    elif full_capacity > 0:
        suggested_actions.extend([
            f"⚠️ {full_capacity} mentors at full capacity, {total - full_capacity} available",
            f"📊 Capacity distribution: {low_occupancy} low, {medium_occupancy} medium, {high_occupancy} high occupancy",
            "🔄 Consider load balancing across available mentors",
            "📈 Review capacity utilization trends"
        ])
    
    if high_occupancy > total * 0.7:
        suggested_actions.append("🔴 High alert: Majority of mentors have high occupancy - urgent capacity expansion needed")
    elif high_occupancy > total * 0.5:
        suggested_actions.append("🟡 Warning: Over half of mentors have high occupancy - consider capacity planning")
    
    capacity_stats["suggested_actions"] = suggested_actions
    capacity_stats["average_occupancy"] = round(candidate_pool['occupancy_ratio'].mean(), 3)
    capacity_stats["median_occupancy"] = round(candidate_pool['occupancy_ratio'].median(), 3)
    capacity_stats["max_occupancy"] = round(candidate_pool['occupancy_ratio'].max(), 3)
    capacity_stats["min_occupancy"] = round(candidate_pool['occupancy_ratio'].min(), 3)
    
    return capacity_stats


def allocate_students_optimized(
    eligibility_matrix_path: str,
    students_path: str, 
    capacity_path: str,
    output_import_sabt_path: str,
    output_allocation_log_path: str,
    allocation_priority_rules: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Optimized main allocation function with comprehensive reporting and memory optimization.
    """
    result = {
        "success": False,
        "statistics": {},
        "warnings": [],
        "errors": [],
        "detailed_metrics": {},
        "allocation_summary": {}
    }
    
    try:
        # Load input files with optimized data types
        eligibility_df = pd.read_excel(eligibility_matrix_path, sheet_name="matrix")
        students_df = pd.read_excel(students_path)
        capacity_df = pd.read_excel(capacity_path)
        
        # Enhanced validation with specific empty dataframe checks and NaN analysis
        is_valid, warnings, errors = validate_inputs_comprehensive(
            eligibility_df, students_df, capacity_df
        )
        
        result["warnings"].extend(warnings)
        result["errors"].extend(errors)
        
        if not is_valid:
            result["message"] = "Input validation failed - please check error details"
            return result
        
        # Build optimized allocation matrix
        matrix_df = build_allocation_matrix(eligibility_df)
        
        # Pre-process capacity data for fast lookups using merge
        capacity_df = capacity_df.reset_index(drop=True)
        
        # Optimize memory usage for large datasets
        students_df = OptimizedAllocationEngine.optimize_dataframe_memory(students_df)
        capacity_df = OptimizedAllocationEngine.optimize_dataframe_memory(capacity_df)
        
        # Process students
        import_sabt_records = []
        allocation_log_records = []
        
        stats = {
            "total_students": len(students_df),
            "successful_allocations": 0,
            "failed_allocations": 0,
            "allocation_rate": 0.0,
            "failure_reasons": {},
            "error_types": {},
            "quality_distribution": {}
        }
        
        for idx, student_row in students_df.iterrows():
            student_dict = student_row.to_dict()
            student_dict["student_id"] = f"student_{idx}"
            
            mentor_record, trace_info = find_mentor_for_student(
                student_dict, matrix_df, capacity_df, allocation_priority_rules
            )
            
            # Use efficient copy instead of deepcopy
            sabt_record = student_dict.copy()
            
            if mentor_record:
                # Successful allocation
                sabt_record.update({
                    "پشتیبان": mentor_record.get("پشتیبان", ""),
                    "کد پستی": mentor_record.get("جایگزین", ""),
                    "کد کارمندی پشتیبان": mentor_record.get("کد کارمندی پشتیبان", ""),
                    "allocation_status": "success",
                    "allocation_quality": trace_info.get("allocation_quality", {}).get("quality_rating", "unknown")
                })
                
                stats["successful_allocations"] += 1
                
                # Track quality distribution
                quality = trace_info.get("allocation_quality", {}).get("quality_rating", "unknown")
                stats["quality_distribution"][quality] = stats["quality_distribution"].get(quality, 0) + 1
                
                log_record = create_success_log_record(student_dict, idx, mentor_record, trace_info)
                
            else:
                # Failed allocation with enhanced error reporting
                sabt_record.update({
                    "پشتیبان": "",
                    "کد پستی": "",
                    "کد کارمندی پشتیبان": "",
                    "allocation_status": "failed",
                    "failure_reason": trace_info.get("reason", "unknown")
                })
                
                stats["failed_allocations"] += 1
                reason = trace_info.get("reason", "unknown")
                error_type = trace_info.get("error_type", "UNKNOWN")
                
                stats["failure_reasons"][reason] = stats["failure_reasons"].get(reason, 0) + 1
                stats["error_types"][error_type] = stats["error_types"].get(error_type, 0) + 1
                
                log_record = create_error_log_record(student_dict, idx, trace_info)
            
            import_sabt_records.append(sabt_record)
            allocation_log_records.append(log_record)
        
        # Calculate final statistics
        stats["allocation_rate"] = stats["successful_allocations"] / stats["total_students"] if stats["total_students"] > 0 else 0
        result["statistics"] = stats
        result["detailed_metrics"] = calculate_detailed_metrics(allocation_log_records)
        result["allocation_summary"] = generate_allocation_summary(allocation_log_records)
        
        # Generate output files with memory optimization
        generate_output_files(
            import_sabt_records, 
            allocation_log_records, 
            stats,
            output_import_sabt_path, 
            output_allocation_log_path
        )
        
        result["success"] = True
        result["message"] = f"Allocation completed: {stats['successful_allocations']}/{stats['total_students']} ({stats['allocation_rate']:.2%})"
        
    except Exception as e:
        error_msg = f"Allocation process failed: {str(e)}"
        result["errors"].append(error_msg)
        result["message"] = error_msg
        
        # Add debug info for troubleshooting
        import traceback
        result["debug_traceback"] = traceback.format_exc()
    
    return result


# Rest of the helper functions (create_success_log_record, create_error_log_record, etc.)
# remain the same as in the previous implementation but can be enhanced similarly

def main():
    """
    Example usage of the fully optimized allocation system.
    """
    # Define file paths
    eligibility_matrix_path = "eligibility_matrix.xlsx"
    students_path = "students.xlsx"
    capacity_path = "capacity.xlsx"
    output_import_sabt_path = "import_to_sabt.xlsx"
    output_allocation_log_path = "allocation_log.xlsx"
    
    # Enhanced priority rules with multiple criteria
    priority_rules = {
        "priority_schools": [12345, 67890],
        "max_occupancy_threshold": 0.95,
        "priority_new_mentors": True,
        "priority_high_capacity": True,
        "mentor_specializations": {
            "ریاضی": 2,
            "علوم": 1,
            "ادبیات": 1
        }
    }
    
    # Perform allocation
    result = allocate_students_optimized(
        eligibility_matrix_path,
        students_path,
        capacity_path,
        output_import_sabt_path,
        output_allocation_log_path,
        allocation_priority_rules=priority_rules
    )
    
    # Display results
    if result["success"]:
        print("✅ Allocation completed successfully!")
        stats = result["statistics"]
        print(f"📊 Allocation Rate: {stats['successful_allocations']}/{stats['total_students']} ({stats['allocation_rate']:.2%})")
        
        # Show quality distribution
        if "quality_distribution" in stats:
            print("🏆 Allocation Quality Distribution:")
            for quality, count in stats["quality_distribution"].items():
                print(f"   - {quality}: {count} students")
        
        if result["warnings"]:
            print("⚠️  Warnings:")
            for warning in result["warnings"]:
                print(f"   - {warning}")
    else:
        print("❌ Allocation failed!")
        for error in result["errors"]:
            print(f"   Error: {error}")
    
    return result["success"]


if __name__ == "__main__":
    main()