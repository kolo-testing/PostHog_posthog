import json
from typing import Any
from django.http import HttpResponse, JsonResponse
from rest_framework import status, serializers, viewsets
from rest_framework.decorators import action
from rest_framework.request import Request

from posthog.api.feature_flag import FeatureFlagSerializer
from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.api.utils import get_token
from django.views.decorators.csrf import csrf_exempt
from posthog.auth import (
    TemporaryTokenAuthentication,
)
from posthog.exceptions import generate_exception_response
from posthog.models import Team, WebExperiment, Experiment
from posthog.utils_cors import cors_response


class WebExperimentsAPISerializer(serializers.ModelSerializer):
    """
    Serializer for the exposed /api/web_experiments endpoint, to be used in posthog-js and for headless APIs.
    """

    feature_flag_key = serializers.CharField(source="feature_flag.key", read_only=True)

    class Meta:
        model = WebExperiment
        fields = ["id", "name", "feature_flag_key", "variants"]

    def validate(self, attrs):
        return attrs

    def create(self, validated_data: dict[str, Any]) -> Any:
        create_params = {
            "name": validated_data.get("name", ""),
            "description": "",
            "type": "web",
            "variants": validated_data.get("variants", None),
            "filters": {
                "events": [{"type": "events", "id": "$pageview", "order": 0, "name": "$pageview"}],
                "layout": "horizontal",
                "date_to": "2024-09-05T23:59",
                "insight": "FUNNELS",
                "interval": "day",
                "date_from": "2024-08-22T10:44",
                "entity_type": "events",
                "funnel_viz_type": "steps",
                "filter_test_accounts": True,
            },
        }

        filters = {
            "groups": [{"properties": [], "rollout_percentage": 100}],
            "multivariate": self.get_variant_names(validated_data)
        }

        feature_flag_serializer = FeatureFlagSerializer(
            data={
                "key": f'{validated_data.get("name")}-feature',
                "name": f'Feature Flag for Experiment {validated_data["name"]}',
                "filters": filters,
                "active": False,
            },
            context=self.context,
        )

        feature_flag_serializer.is_valid(raise_exception=True)
        feature_flag = feature_flag_serializer.save()

        experiment = Experiment.objects.create(
            team_id=self.context["team_id"], feature_flag=feature_flag, **create_params
        )
        return experiment

    def update(self, instance: WebExperiment, validated_data: dict[str, Any]) -> Any:
        variants = validated_data.get("variants", None)
        if variants is not None and isinstance(variants, dict):
            feature_flag = instance.feature_flag
            filters = {
                "groups": feature_flag.filters.get("groups", None),
                "multivariate": self.get_variant_names(validated_data)
            }

            existing_flag_serializer = FeatureFlagSerializer(
                feature_flag,
                data={"filters": filters},
                partial=True,
                context=self.context,
            )
            existing_flag_serializer.is_valid(raise_exception=True)
            existing_flag_serializer.save()

        instance = super().update(instance, validated_data)
        return instance

    def get_variant_names(self, validated_data: dict[str, Any]):
        variant_names = []
        variants = validated_data.get("variants", None)
        if variants is not None and isinstance(variants, dict):
            for variant, transforms in variants.items():
                variant_names.append(
                    {"key": variant, "rollout_percentage": transforms.get("rollout_percentage", 0)}
                )
        return {"variants": variant_names}


class WebExperimentViewSet(TeamAndOrgViewSetMixin, viewsets.ModelViewSet):
    scope_object = "experiment"
    serializer_class = WebExperimentsAPISerializer
    authentication_classes = [TemporaryTokenAuthentication]
    queryset = WebExperiment.objects.select_related("feature_flag").all()


@csrf_exempt
@action(methods=["GET"], detail=True)
def web_experiments(request: Request):
    token = get_token(None, request)
    if request.method == "OPTIONS":
        return cors_response(request, HttpResponse(""))
    if not token:
        return cors_response(
            request,
            generate_exception_response(
                "experiments",
                "API key not provided. You can find your project API key in your PostHog project settings.",
                type="authentication_error",
                code="missing_api_key",
                status_code=status.HTTP_401_UNAUTHORIZED,
            ),
        )

    if request.method == "GET":
        team = Team.objects.get_team_from_cache_or_token(token)
        if team is None:
            return cors_response(
                request,
                generate_exception_response(
                    "experiments",
                    "Project API key invalid. You can find your project API key in your PostHog project settings.",
                    type="authentication_error",
                    code="invalid_api_key",
                    status_code=status.HTTP_401_UNAUTHORIZED,
                ),
            )

        result = WebExperimentsAPISerializer(
            WebExperiment.objects.filter(team_id=team.id).exclude(archived=True).select_related("feature_flag"),
            many=True,
        ).data

        return cors_response(request, JsonResponse({"experiments": result}))
